#include <Arduino.h>
#include <Wire.h>
#include <WiFi.h>
#include <LittleFS.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include <TinyGPSPlus.h>
#include <M5Unified.h>
#include <driver/twai.h>

#include "coworker_protocol.h"

using jf_coworker::LinkMode;
using jf_coworker::ReplyChannel;

namespace {

constexpr char kCfgPath[] = "/coworker.toml";
constexpr uint8_t kSht4xMeasureHighPrecision = 0xFD;
constexpr uint8_t kBmp280ChipId = 0x58;
constexpr uint32_t kSensorRefreshMs = 1000;
constexpr uint32_t kCacheFreshMs = 4000;
constexpr int kPortA1Tx = 2;
constexpr int kPortA1Rx = 1;
constexpr int kPortA2Sda = 38;
constexpr int kPortA2Scl = 39;
constexpr int kGpsTx = 5;
constexpr int kGpsRx = 6;
constexpr size_t kMqttMaxFields = 48;
constexpr size_t kMqttKeyLen = 48;
constexpr size_t kMqttValueLen = 48;
constexpr size_t kMqttRawCacheSlots = 128;
constexpr size_t kMqttRawTopicLen = 96;
constexpr size_t kMqttRawPayloadLen = 768;
constexpr size_t kMqttClientBufferSize = 1024;
constexpr uint32_t kMqttReadWaitMs = 1200;

HardwareSerial LinkSerial(1);
HardwareSerial GpsSerial(2);
WiFiClient WifiClient;
PubSubClient MqttClient(WifiClient);
TinyGPSPlus Gps;

struct CoworkerConfig {
  bool wifiJoinEnabled = true;
  String wifiSsid = "TG10-01250";
  String wifiPassword = "licorenv";
  bool mqttEnabled = true;
  String mqttName = "LI7820";
  String mqttUriHost = "192.168.10.1";
  uint16_t mqttPort = 1883;
  String mqttCacheTopics = "licor";
  String mqttTopics = "licor/niobrara/output/concentration";
  bool gpsEnabled = true;
  uint32_t gpsBaud = 9600;
  LinkMode linkMode = LinkMode::RS485;
  uint8_t nodeId = 33;
  uint8_t hubNodeId = 17;
  uint32_t rs485Baud = 115200;
  int rs485DirGpio = -1;
  uint32_t canBitrate = 500000;
  bool debugEnabled = true;
  bool debugVerbose = false;
  String sht4xAddrs = "44";
  String bmp280Addrs = "76";
};

struct MqttFieldSample {
  bool used = false;
  char key[kMqttKeyLen] = {0};
  char value[kMqttValueLen] = {0};
  uint32_t sampleMs = 0;
};

struct MqttRawSample {
  bool used = false;
  char topic[kMqttRawTopicLen] = {0};
  char payload[kMqttRawPayloadLen] = {0};
  uint32_t sampleMs = 0;
  uint32_t seq = 0;
  uint32_t firstSeq = 0;
};

enum class CommandSource {
  Local,
  Rs485,
  Can,
};

struct MqttSnapshot {
  bool connected = false;
  String lastTopic;
  String lastPayload;
  String lastRxLine;
  String lastN2o;
  String lastH2o;
  String lastDiag;
  uint32_t lastRxMs = 0;
  uint32_t schemaRev = 1;
  size_t fieldCount = 0;
  MqttFieldSample fields[kMqttMaxFields];
  MqttRawSample raw[kMqttRawCacheSlots];
  uint32_t rawSeq = 0;
  JsonDocument cacheDoc;
  uint32_t cacheRev = 0;
};

struct GpsSnapshot {
  bool fix = false;
  double lat = 0.0;
  double lon = 0.0;
  double altM = 0.0;
  uint32_t sats = 0;
  uint32_t lastRxMs = 0;
  String utcTime;
};

struct I2cSummary {
  bool sht4xPresent = false;
  bool bmp280Present = false;
  String detectedAddrs;
};

struct Bmp280Calib {
  uint16_t digT1 = 0;
  int16_t digT2 = 0;
  int16_t digT3 = 0;
  uint16_t digP1 = 0;
  int16_t digP2 = 0;
  int16_t digP3 = 0;
  int16_t digP4 = 0;
  int16_t digP5 = 0;
  int16_t digP6 = 0;
  int16_t digP7 = 0;
  int16_t digP8 = 0;
  int16_t digP9 = 0;
};

struct Sht4xCache {
  bool valid = false;
  uint8_t addr = 0;
  float tempC = 0.0f;
  float rhPct = 0.0f;
  uint32_t sampleMs = 0;
};

struct Bmp280Cache {
  bool valid = false;
  uint8_t addr = 0;
  float tempC = 0.0f;
  float pressurePa = 0.0f;
  uint32_t sampleMs = 0;
};

CoworkerConfig g_cfg;
MqttSnapshot g_mqtt;
GpsSnapshot g_gps;
I2cSummary g_i2c;
String g_rs485RxLine;
String g_canRxLine;
String g_usbRxLine;
String g_rs485ReplyPrefix;
uint8_t g_canRxSeq = 0;
bool g_linkReady = false;
esp_err_t g_canLastInitErr = ESP_OK;
esp_err_t g_canLastTxErr = ESP_OK;
uint32_t g_canRxFrames = 0;
uint32_t g_canRxSeqErrors = 0;
uint32_t g_canRxAddrReject = 0;
bool g_prevWifiConnected = false;
bool g_prevMqttConnected = false;
bool g_prevGpsFix = false;
uint32_t g_lastSensorRefreshMs = 0;
Sht4xCache g_sht4x;
Bmp280Cache g_bmp280;
CommandSource g_commandSource = CommandSource::Local;

String trimCopy(const String &input) {
  String out = input;
  out.trim();
  return out;
}

String modeToString(LinkMode mode) {
  return mode == LinkMode::CAN ? "can" : "rs485";
}

LinkMode parseMode(const String &value) {
  return value.equalsIgnoreCase("can") ? LinkMode::CAN : LinkMode::RS485;
}

bool parseTomlBool(const String &value) {
  return value == "1" || value.equalsIgnoreCase("true") || value.equalsIgnoreCase("yes");
}

String parseConfigStringValue(const String &raw) {
  String out = raw;
  out.trim();
  if (out.length() >= 2 && out[0] == '"' && out[out.length() - 1] == '"') {
    String inner = out.substring(1, out.length() - 1);
    String unescaped;
    unescaped.reserve(inner.length());
    bool escaped = false;
    for (size_t i = 0; i < inner.length(); ++i) {
      const char ch = inner[i];
      if (escaped) {
        unescaped += ch;
        escaped = false;
      } else if (ch == '\\') {
        escaped = true;
      } else {
        unescaped += ch;
      }
    }
    if (escaped) unescaped += '\\';
    out = unescaped;
  }
  return out;
}

String tomlEscape(const String &value) {
  String out;
  for (size_t i = 0; i < value.length(); ++i) {
    char ch = value[i];
    if (ch == '\\' || ch == '"') out += '\\';
    out += ch;
  }
  return out;
}

void splitCsv(const String &csv, String *out, size_t cap, size_t &count) {
  count = 0;
  int start = 0;
  while (start < csv.length() && count < cap) {
    int comma = csv.indexOf(',', start);
    String token = comma >= 0 ? csv.substring(start, comma) : csv.substring(start);
    token.trim();
    if (!token.isEmpty()) out[count++] = token;
    if (comma < 0) break;
    start = comma + 1;
  }
}

bool parseTopicList(const String &csv, String *out, size_t cap, size_t &count) {
  count = 0;
  if (cap == 0) return false;

  String token;
  int bracketDepth = 0;
  auto pushTopic = [&](const String &raw) {
    String topic = raw;
    topic.trim();
    if (topic.isEmpty()) return;
    for (size_t i = 0; i < count; ++i) {
      if (out[i] == topic) return;
    }
    if (count < cap) out[count++] = topic;
  };

  auto expandToken = [&](const String &raw) {
    String item = raw;
    item.trim();
    const int open = item.indexOf('[');
    const int close = item.indexOf(']', open + 1);
    if (open >= 0 && close > open + 1) {
      String prefix = item.substring(0, open);
      String suffix = item.substring(close + 1);
      prefix.trim();
      suffix.trim();
      String inner = item.substring(open + 1, close);
      int start = 0;
      while (start <= inner.length()) {
        int comma = inner.indexOf(',', start);
        String part = comma >= 0 ? inner.substring(start, comma) : inner.substring(start);
        part.trim();
        if (!part.isEmpty()) pushTopic(prefix + part + suffix);
        if (comma < 0) break;
        start = comma + 1;
      }
      return;
    }
    pushTopic(item);
  };

  for (size_t i = 0; i <= csv.length(); ++i) {
    const char ch = i < csv.length() ? csv[i] : '\0';
    const bool split = (ch == ',' && bracketDepth == 0) || ch == '\0';
    if (split) {
      expandToken(token);
      token = "";
      continue;
    }
    if (ch == '[') bracketDepth++;
    else if (ch == ']' && bracketDepth > 0) bracketDepth--;
    token += ch;
  }

  return count > 0;
}

String formatAgeMs(uint32_t timestampMs) {
  if (timestampMs == 0) return "-1";
  return String(millis() - timestampMs);
}

String sanitizeResponseValue(const String &input, size_t maxLen = 160) {
  String out;
  out.reserve(input.length());
  for (size_t i = 0; i < input.length() && out.length() < maxLen; ++i) {
    char ch = input[i];
    if (ch == '\r' || ch == '\n' || ch == '\0') continue;
    if (ch == '!') ch = '?';
    out += ch;
  }
  return out;
}

String normalizeTopicPath(const String &input) {
  String out = input;
  out.trim();
  while (out.length() > 1 && out.endsWith("/")) {
    out.remove(out.length() - 1);
  }
  return out;
}

void mqttAppendUniqueTopic(const String &topic, String *out, size_t cap, size_t &count) {
  if (cap == 0) return;
  const String normalized = normalizeTopicPath(topic);
  if (normalized.isEmpty()) return;
  for (size_t i = 0; i < count; ++i) {
    if (normalizeTopicPath(out[i]) == normalized) {
      return;
    }
  }
  if (count < cap) {
    out[count++] = normalized;
  }
}

String mqttCollapseRepeatedPrefix(const String &topic) {
  const String normalized = normalizeTopicPath(topic);
  if (normalized.isEmpty()) return normalized;

  for (int slash = normalized.indexOf('/'); slash > 0; slash = normalized.indexOf('/', slash + 1)) {
    const String prefix = normalized.substring(0, slash);
    const String duplicatedStart = prefix + "/" + prefix;
    if (normalized.startsWith(duplicatedStart)) {
      const String remaining = normalized.substring(duplicatedStart.length());
      if (remaining.isEmpty()) {
        return normalizeTopicPath(prefix);
      }
      return normalizeTopicPath(prefix + remaining);
    }
  }

  return normalized;
}

void mqttBuildTopicCandidates(const String &requestedTopic, String *out, size_t cap, size_t &count) {
  count = 0;
  const String normalized = normalizeTopicPath(requestedTopic);
  const String collapsed = mqttCollapseRepeatedPrefix(normalized);

  auto addWithAncestors = [&](const String &seed) {
    String current = normalizeTopicPath(seed);
    while (!current.isEmpty()) {
      mqttAppendUniqueTopic(current, out, cap, count);
      const int slash = current.lastIndexOf('/');
      if (slash <= 0) break;
      current = current.substring(0, slash);
    }
  };

  addWithAncestors(normalized);
  if (collapsed != normalized) {
    addWithAncestors(collapsed);
  }
}

bool mqttConfiguredPathMatchesTopic(const String &configuredPath, const String &topic) {
  const String configured = normalizeTopicPath(configuredPath);
  const String normalizedTopic = normalizeTopicPath(topic);
  if (configured.isEmpty() || normalizedTopic.isEmpty()) {
    return false;
  }

  if (configured.endsWith("/#")) {
    const String prefix = configured.substring(0, configured.length() - 2);
    return normalizedTopic == prefix || normalizedTopic.startsWith(prefix + "/");
  }

  if (configured.endsWith("/+") || configured == "+") {
    const int slash = configured.lastIndexOf('/');
    const String prefix = slash >= 0 ? configured.substring(0, slash) : String("");
    if (!prefix.isEmpty()) {
      if (!normalizedTopic.startsWith(prefix + "/")) return false;
      const String suffix = normalizedTopic.substring(prefix.length() + 1);
      return suffix.indexOf('/') < 0;
    }
    return normalizedTopic.indexOf('/') < 0;
  }

  return normalizedTopic == configured || normalizedTopic.startsWith(configured + "/");
}

bool mqttConfiguredListMatchesTopic(const String &listCsv, const String &topic) {
  String configured[16];
  size_t count = 0;
  if (!parseTopicList(listCsv, configured, 16, count)) {
    return false;
  }

  for (size_t i = 0; i < count; ++i) {
    if (mqttConfiguredPathMatchesTopic(configured[i], topic)) {
      return true;
    }
  }
  return false;
}

void mqttAppendSubscriptionRoots(const String &listCsv, String *out, size_t cap, size_t &count) {
  String configured[16];
  size_t configuredCount = 0;
  if (!parseTopicList(listCsv, configured, 16, configuredCount)) {
    return;
  }

  for (size_t i = 0; i < configuredCount; ++i) {
    const String normalized = normalizeTopicPath(configured[i]);
    mqttAppendUniqueTopic(normalized, out, cap, count);

    const int slash = normalized.lastIndexOf('/');
    if (slash > 0) {
      mqttAppendUniqueTopic(normalized.substring(0, slash), out, cap, count);
    }
  }
}

String sanitizeMqttKey(const String &input, size_t maxLen = kMqttKeyLen - 1) {
  String out;
  out.reserve(input.length());
  bool lastSep = false;
  for (size_t i = 0; i < input.length() && out.length() < maxLen; ++i) {
    const char ch = input[i];
    if (isalnum(static_cast<unsigned char>(ch))) {
      out += static_cast<char>(toupper(static_cast<unsigned char>(ch)));
      lastSep = false;
    } else if (!lastSep) {
      out += '_';
      lastSep = true;
    }
  }
  while (out.endsWith("_")) {
    out.remove(out.length() - 1);
  }
  return out;
}

bool isFresh(uint32_t timestampMs, uint32_t maxAgeMs = kCacheFreshMs) {
  return timestampMs != 0 && millis() - timestampMs <= maxAgeMs;
}

String hexAddr(uint8_t addr) {
  char buf[5];
  snprintf(buf, sizeof(buf), "%02X", addr);
  return String(buf);
}

String mqttPollFields();

void appendPollField(String &out, const String &key, const String &value) {
  if (!out.isEmpty()) out += ',';
  out += key;
  out += '=';
  out += value;
}

void appendGpsPollFields(String &out) {
  if (!g_cfg.gpsEnabled) return;

  const bool gpsFresh = isFresh(g_gps.lastRxMs);
  appendPollField(out, "GPS_fix", String(gpsFresh && g_gps.fix ? 1 : 0));
  appendPollField(out, "GPS_sats", gpsFresh ? String(g_gps.sats) : String("-"));
  appendPollField(out, "GPS_latitude", gpsFresh && g_gps.fix ? String(g_gps.lat, 6) : String("-"));
  appendPollField(out, "GPS_longitude", gpsFresh && g_gps.fix ? String(g_gps.lon, 6) : String("-"));
  appendPollField(out, "GPS_time", gpsFresh && !g_gps.utcTime.isEmpty() ? g_gps.utcTime : String("-"));
}

void appendSht4xPollFields(String &out) {
  String tokens[8];
  size_t count = 0;
  splitCsv(g_cfg.sht4xAddrs, tokens, 8, count);
  if (count == 0) return;

  const bool shtOk = g_sht4x.valid && isFresh(g_sht4x.sampleMs);
  for (size_t i = 0; i < count; ++i) {
    const uint8_t addr = static_cast<uint8_t>(strtoul(tokens[i].c_str(), nullptr, 16));
    const String addrTag = hexAddr(addr);
    const bool addrMatch = shtOk && addr == g_sht4x.addr;
    appendPollField(out, "SHT4X_" + addrTag + "_temp_c", addrMatch ? String(g_sht4x.tempC, 2) : String("-"));
    appendPollField(out, "SHT4X_" + addrTag + "_rh_pct", addrMatch ? String(g_sht4x.rhPct, 2) : String("-"));
  }
}

void appendBmp280PollFields(String &out) {
  String tokens[8];
  size_t count = 0;
  splitCsv(g_cfg.bmp280Addrs, tokens, 8, count);
  if (count == 0) return;

  const bool bmpOk = g_bmp280.valid && isFresh(g_bmp280.sampleMs);
  for (size_t i = 0; i < count; ++i) {
    const uint8_t addr = static_cast<uint8_t>(strtoul(tokens[i].c_str(), nullptr, 16));
    const String addrTag = hexAddr(addr);
    const bool addrMatch = bmpOk && addr == g_bmp280.addr;
    appendPollField(out, "BMP280_" + addrTag + "_temp_c", addrMatch ? String(g_bmp280.tempC, 2) : String("-"));
    appendPollField(out, "BMP280_" + addrTag + "_press_pa", addrMatch ? String(g_bmp280.pressurePa, 1) : String("-"));
  }
}

String buildPollAllFields() {
  String out;
  const bool mqttFresh = isFresh(g_mqtt.lastRxMs);
  appendPollField(out, "MQTT_age_ms", mqttFresh ? formatAgeMs(g_mqtt.lastRxMs) : String("-"));
  appendGpsPollFields(out);
  appendSht4xPollFields(out);
  appendBmp280PollFields(out);
  out += mqttPollFields();
  return out;
}

bool parseRs485Envelope(const String &line, String &commandOut, String &replyPrefixOut) {
  if (!line.startsWith("@>")) {
    commandOut = line;
    replyPrefixOut = "";
    return true;
  }

  const int colon = line.indexOf(':', 2);
  if (colon < 0) {
    return false;
  }

  const String addrText = line.substring(2, colon);
  if (addrText.isEmpty()) {
    return false;
  }

  const long targetAddr = strtol(addrText.c_str(), nullptr, 10);
  if (targetAddr < 0 || targetAddr > 255) {
    return false;
  }

  commandOut = trimCopy(line.substring(colon + 1));
  if (commandOut.isEmpty()) {
    return false;
  }

  if (targetAddr == 0 || targetAddr == 255) {
    replyPrefixOut = "";
    return false;
  }

  if (static_cast<uint8_t>(targetAddr) != g_cfg.nodeId) {
    replyPrefixOut = "";
    return false;
  }

  replyPrefixOut = "@<" + String(g_cfg.nodeId) + ':';
  return true;
}

void splitTopLevelCsv(const String &text, String *out, size_t cap, size_t &count) {
  count = 0;
  String token;
  int bracketDepth = 0;
  int braceDepth = 0;
  for (size_t i = 0; i <= text.length(); ++i) {
    const char ch = i < text.length() ? text[i] : '\0';
    const bool split = (ch == ',' && bracketDepth == 0 && braceDepth == 0) || ch == '\0';
    if (split) {
      token.trim();
      if (!token.isEmpty() && count < cap) out[count++] = token;
      token = "";
      continue;
    }
    if (ch == '[') bracketDepth++;
    else if (ch == ']' && bracketDepth > 0) bracketDepth--;
    else if (ch == '{') braceDepth++;
    else if (ch == '}' && braceDepth > 0) braceDepth--;
    token += ch;
  }
}

String mqttTopicLeaf(const String &topic) {
  const int slash = topic.lastIndexOf('/');
  if (slash < 0) return topic;
  return topic.substring(slash + 1);
}

String mqttRelativeTopicPath(const String &topic) {
  String configured[16];
  size_t count = 0;
  parseTopicList(g_cfg.mqttTopics, configured, 16, count);

  size_t bestLen = 0;
  String bestRelative;
  for (size_t i = 0; i < count; ++i) {
    const String &candidate = configured[i];
    String relative;
    bool matched = false;
    if (candidate.endsWith("/")) {
      if (topic.startsWith(candidate)) {
        relative = topic.substring(candidate.length());
        matched = true;
      }
    } else if (topic == candidate) {
      matched = true;
    } else {
      const String prefix = candidate + "/";
      if (topic.startsWith(prefix)) {
        relative = topic.substring(prefix.length());
        matched = true;
      }
    }
    if (matched && candidate.length() >= bestLen) {
      bestLen = candidate.length();
      bestRelative = relative;
    }
  }

  bestRelative.trim();
  if (bestRelative.equalsIgnoreCase("data")) {
    return "";
  }
  return bestRelative;
}

String mqttNamePrefix() {
  const String key = sanitizeMqttKey(g_cfg.mqttName, 23);
  return key.isEmpty() ? String("LI7820") : key;
}

bool mqttIsTopicInList(const String &listCsv, const String &topic);
bool mqttTopicMatchesRequest(const String &requestTopic, const String &cachedTopic);
String mqttPollFieldsFromCache();
bool mqttReadRawTopicFromCache(const String &requestedTopic, String &payloadOut);

int mqttFieldIndex(const String &sanitizedKey) {
  if (sanitizedKey.isEmpty()) return -1;
  for (size_t i = 0; i < g_mqtt.fieldCount; ++i) {
    if (sanitizedKey == g_mqtt.fields[i].key) {
      return static_cast<int>(i);
    }
  }
  return -1;
}

bool isHeaderFieldKey(const String &sanitizedKey) {
  return sanitizedKey == "HEADER" || sanitizedKey.startsWith("HEADER_");
}

String asciiLower(const String &input) {
  String out = input;
  for (size_t i = 0; i < out.length(); ++i) {
    out[i] = static_cast<char>(tolower(static_cast<unsigned char>(out[i])));
  }
  return out;
}

void mqttStoreFieldValue(const String &rawKey, const String &rawValue, uint32_t sampleMs) {
  const String key = sanitizeMqttKey(rawKey);
  if (key.isEmpty()) return;

  // LI7820 HEADER fields are metadata duplicates for this use case; skip them.
  if (isHeaderFieldKey(key)) return;

  int idx = mqttFieldIndex(key);
  if (idx < 0) {
    if (g_mqtt.fieldCount >= kMqttMaxFields) return;
    idx = static_cast<int>(g_mqtt.fieldCount++);
    strlcpy(g_mqtt.fields[idx].key, key.c_str(), sizeof(g_mqtt.fields[idx].key));
    g_mqtt.schemaRev++;
  }

  strlcpy(g_mqtt.fields[idx].value, sanitizeResponseValue(rawValue, kMqttValueLen - 1).c_str(), sizeof(g_mqtt.fields[idx].value));
  g_mqtt.fields[idx].sampleMs = sampleMs;
  g_mqtt.fields[idx].used = true;
}

String mqttFieldValue(const char *key) {
  if (!key || !key[0]) return "";
  for (size_t i = 0; i < g_mqtt.fieldCount; ++i) {
    if (strcmp(g_mqtt.fields[i].key, key) == 0) {
      return String(g_mqtt.fields[i].value);
    }
  }
  return "";
}

String mqttJsonLiteral(const JsonVariantConst &value, size_t maxLen = 160) {
  String out;
  serializeJson(value, out);
  return sanitizeResponseValue(out, maxLen);
}

void mqttCollectJsonFields(const JsonVariantConst &value, const String &topicKey, const String &path, uint32_t sampleMs) {
  if (value.is<JsonObjectConst>()) {
    JsonObjectConst obj = value.as<JsonObjectConst>();
    for (JsonPairConst kv : obj) {
      String nextPath = path;
      if (!nextPath.isEmpty()) nextPath += '_';
      nextPath += kv.key().c_str();
      mqttCollectJsonFields(kv.value(), topicKey, nextPath, sampleMs);
    }
    return;
  }

  if (value.is<JsonArrayConst>()) {
    JsonArrayConst arr = value.as<JsonArrayConst>();
    size_t idx = 0;
    for (JsonVariantConst item : arr) {
      String nextPath = path;
      if (!nextPath.isEmpty()) {
        nextPath += '_';
      }
      nextPath += String(idx);
      mqttCollectJsonFields(item, topicKey, nextPath, sampleMs);
      idx++;
    }
    return;
  }

  String fieldKey;
  if (!topicKey.isEmpty() && !path.isEmpty()) {
    fieldKey = topicKey + '_' + path;
  } else if (!path.isEmpty()) {
    fieldKey = path;
  } else if (!topicKey.isEmpty()) {
    fieldKey = topicKey;
  } else {
    fieldKey = "VALUE";
  }

  String valueText;
  if (value.is<bool>()) {
    valueText = value.as<bool>() ? "1" : "0";
  } else if (value.is<long long>()) {
    valueText = String(value.as<long long>());
  } else if (value.is<unsigned long long>()) {
    valueText = String(static_cast<unsigned long>(value.as<unsigned long long>()));
  } else if (value.is<float>() || value.is<double>()) {
    valueText = String(value.as<double>(), 6);
  } else if (value.is<const char *>()) {
    valueText = value.as<const char *>();
  } else if (value.is<String>()) {
    valueText = value.as<String>();
  } else {
    return;
  }

  mqttStoreFieldValue(fieldKey, valueText, sampleMs);
}

void mqttCollectMessageFields(const String &topic, const String &payload) {
  const uint32_t sampleMs = millis();
  String topicKey = sanitizeMqttKey(mqttRelativeTopicPath(topic));
  if (topicKey.isEmpty()) {
    const String leaf = sanitizeMqttKey(mqttTopicLeaf(topic));
    if (!leaf.equalsIgnoreCase("DATA") && !leaf.equalsIgnoreCase("CONCENTRATION")) {
      topicKey = leaf;
    }
  }

  JsonDocument doc;
  const DeserializationError err = deserializeJson(doc, payload);
  if (!err) {
    mqttCollectJsonFields(doc.as<JsonVariantConst>(), topicKey, "", sampleMs);
  } else {
    String scalarKey = topicKey;
    if (scalarKey.isEmpty()) {
      scalarKey = sanitizeMqttKey(mqttTopicLeaf(topic));
    }
    if (scalarKey.isEmpty()) {
      scalarKey = "VALUE";
    }
    mqttStoreFieldValue(scalarKey, payload, sampleMs);
  }

  g_mqtt.lastN2o = mqttFieldValue("N2O");
  g_mqtt.lastH2o = mqttFieldValue("H2O");
  g_mqtt.lastDiag = mqttFieldValue("DIAG");
}

String mqttPollFields() {
  const String cacheFields = mqttPollFieldsFromCache();
  if (!cacheFields.isEmpty()) {
    return cacheFields;
  }

  if (!isFresh(g_mqtt.lastRxMs)) {
    return "";
  }

  String out;
  const String namePrefix = mqttNamePrefix();
  for (size_t i = 0; i < g_mqtt.fieldCount; ++i) {
    if (!g_mqtt.fields[i].used) continue;
    out += ',';
    out += namePrefix;
    out += '_';
    out += asciiLower(String(g_mqtt.fields[i].key));
    out += '=';
    out += isFresh(g_mqtt.fields[i].sampleMs) ? String(g_mqtt.fields[i].value) : String("-");
  }
  return out;
}

String buildMqttRxLine() {
  const String topic = g_mqtt.lastTopic.isEmpty() ? String("-") : sanitizeResponseValue(g_mqtt.lastTopic, 96);
  const String payload = g_mqtt.lastPayload.isEmpty() ? String("-") : sanitizeResponseValue(g_mqtt.lastPayload, 720);
  return "@MQTT=RX:TOPIC=" + topic + ",NAME=" + sanitizeResponseValue(g_cfg.mqttName, 24) + ",PAYLOAD=" + payload + "!";
}

bool readConfiguredSht4x(float &tempC, float &rhPct, uint8_t &addrOut);
bool readConfiguredBmp280(float &tempC, float &pressurePa, uint8_t &addrOut);

void refreshSensorCaches(bool force = false) {
  if (!force && millis() - g_lastSensorRefreshMs < kSensorRefreshMs) return;
  g_lastSensorRefreshMs = millis();

  float shtTempC = 0.0f;
  float shtRhPct = 0.0f;
  uint8_t shtAddr = 0;
  if (readConfiguredSht4x(shtTempC, shtRhPct, shtAddr)) {
    g_sht4x.valid = true;
    g_sht4x.addr = shtAddr;
    g_sht4x.tempC = shtTempC;
    g_sht4x.rhPct = shtRhPct;
    g_sht4x.sampleMs = millis();
  } else {
    g_sht4x.valid = false;
  }

  float bmpTempC = 0.0f;
  float bmpPressurePa = 0.0f;
  uint8_t bmpAddr = 0;
  if (readConfiguredBmp280(bmpTempC, bmpPressurePa, bmpAddr)) {
    g_bmp280.valid = true;
    g_bmp280.addr = bmpAddr;
    g_bmp280.tempC = bmpTempC;
    g_bmp280.pressurePa = bmpPressurePa;
    g_bmp280.sampleMs = millis();
  } else {
    g_bmp280.valid = false;
  }
}

bool i2cProbeAddr(uint8_t addr) {
  Wire.beginTransmission(addr);
  return Wire.endTransmission() == 0;
}

bool i2cReadReg(uint8_t addr, uint8_t reg, uint8_t *data, size_t len) {
  Wire.beginTransmission(addr);
  Wire.write(reg);
  if (Wire.endTransmission(false) != 0) return false;
  const size_t got = Wire.requestFrom(static_cast<int>(addr), static_cast<int>(len));
  if (got != len) return false;
  for (size_t i = 0; i < len; ++i) data[i] = static_cast<uint8_t>(Wire.read());
  return true;
}

bool i2cWriteBytes(uint8_t addr, const uint8_t *data, size_t len) {
  Wire.beginTransmission(addr);
  for (size_t i = 0; i < len; ++i) Wire.write(data[i]);
  return Wire.endTransmission() == 0;
}

bool readConfiguredSht4x(float &tempC, float &rhPct, uint8_t &addrOut);
bool readConfiguredBmp280(float &tempC, float &pressurePa, uint8_t &addrOut);

uint8_t sht4xCrc8(const uint8_t *data, size_t len) {
  uint8_t crc = 0xFF;
  for (size_t i = 0; i < len; ++i) {
    crc ^= data[i];
    for (int bit = 0; bit < 8; ++bit) {
      crc = (crc & 0x80) ? static_cast<uint8_t>((crc << 1) ^ 0x31) : static_cast<uint8_t>(crc << 1);
    }
  }
  return crc;
}

bool readSht4xAddr(uint8_t addr, float &tempC, float &rhPct) {
  if (!i2cProbeAddr(addr)) return false;
  const uint8_t cmd = kSht4xMeasureHighPrecision;
  if (!i2cWriteBytes(addr, &cmd, 1)) return false;
  delay(10);
  uint8_t data[6] = {0};
  const size_t got = Wire.requestFrom(static_cast<int>(addr), 6);
  if (got != 6) return false;
  for (size_t i = 0; i < 6; ++i) data[i] = static_cast<uint8_t>(Wire.read());
  if (sht4xCrc8(&data[0], 2) != data[2] || sht4xCrc8(&data[3], 2) != data[5]) return false;
  const uint16_t rawTemp = static_cast<uint16_t>((data[0] << 8) | data[1]);
  const uint16_t rawRh = static_cast<uint16_t>((data[3] << 8) | data[4]);
  tempC = -45.0f + 175.0f * (static_cast<float>(rawTemp) / 65535.0f);
  rhPct = -6.0f + 125.0f * (static_cast<float>(rawRh) / 65535.0f);
  if (rhPct < 0.0f) rhPct = 0.0f;
  if (rhPct > 100.0f) rhPct = 100.0f;
  return true;
}

bool readConfiguredSht4x(float &tempC, float &rhPct, uint8_t &addrOut) {
  String tokens[8];
  size_t count = 0;
  splitCsv(g_cfg.sht4xAddrs, tokens, 8, count);
  for (size_t i = 0; i < count; ++i) {
    const uint8_t addr = static_cast<uint8_t>(strtoul(tokens[i].c_str(), nullptr, 16));
    if (readSht4xAddr(addr, tempC, rhPct)) {
      addrOut = addr;
      return true;
    }
  }
  return false;
}

bool bmp280LoadCalib(uint8_t addr, Bmp280Calib &calib) {
  uint8_t raw[24] = {0};
  if (!i2cReadReg(addr, 0x88, raw, sizeof(raw))) return false;
  calib.digT1 = static_cast<uint16_t>((raw[1] << 8) | raw[0]);
  calib.digT2 = static_cast<int16_t>((raw[3] << 8) | raw[2]);
  calib.digT3 = static_cast<int16_t>((raw[5] << 8) | raw[4]);
  calib.digP1 = static_cast<uint16_t>((raw[7] << 8) | raw[6]);
  calib.digP2 = static_cast<int16_t>((raw[9] << 8) | raw[8]);
  calib.digP3 = static_cast<int16_t>((raw[11] << 8) | raw[10]);
  calib.digP4 = static_cast<int16_t>((raw[13] << 8) | raw[12]);
  calib.digP5 = static_cast<int16_t>((raw[15] << 8) | raw[14]);
  calib.digP6 = static_cast<int16_t>((raw[17] << 8) | raw[16]);
  calib.digP7 = static_cast<int16_t>((raw[19] << 8) | raw[18]);
  calib.digP8 = static_cast<int16_t>((raw[21] << 8) | raw[20]);
  calib.digP9 = static_cast<int16_t>((raw[23] << 8) | raw[22]);
  return calib.digP1 != 0;
}

bool readBmp280Addr(uint8_t addr, float &tempC, float &pressurePa) {
  if (!i2cProbeAddr(addr)) return false;
  uint8_t chipId = 0;
  if (!i2cReadReg(addr, 0xD0, &chipId, 1) || chipId != kBmp280ChipId) return false;
  const uint8_t ctrlMeas[2] = {0xF4, 0x27};
  const uint8_t config[2] = {0xF5, 0xA0};
  if (!i2cWriteBytes(addr, ctrlMeas, sizeof(ctrlMeas))) return false;
  if (!i2cWriteBytes(addr, config, sizeof(config))) return false;
  delay(10);

  Bmp280Calib calib;
  if (!bmp280LoadCalib(addr, calib)) return false;

  uint8_t data[6] = {0};
  if (!i2cReadReg(addr, 0xF7, data, sizeof(data))) return false;

  const int32_t adcP = static_cast<int32_t>((((uint32_t)data[0] << 16) | ((uint32_t)data[1] << 8) | data[2]) >> 4);
  const int32_t adcT = static_cast<int32_t>((((uint32_t)data[3] << 16) | ((uint32_t)data[4] << 8) | data[5]) >> 4);

  int32_t var1 = ((((adcT >> 3) - (static_cast<int32_t>(calib.digT1) << 1))) * static_cast<int32_t>(calib.digT2)) >> 11;
  int32_t var2 = (((((adcT >> 4) - static_cast<int32_t>(calib.digT1)) * ((adcT >> 4) - static_cast<int32_t>(calib.digT1))) >> 12) * static_cast<int32_t>(calib.digT3)) >> 14;
  const int32_t tFine = var1 + var2;
  const int32_t t = (tFine * 5 + 128) >> 8;
  tempC = static_cast<float>(t) / 100.0f;

  int64_t pVar1 = static_cast<int64_t>(tFine) - 128000;
  int64_t pVar2 = pVar1 * pVar1 * static_cast<int64_t>(calib.digP6);
  pVar2 += (pVar1 * static_cast<int64_t>(calib.digP5)) << 17;
  pVar2 += static_cast<int64_t>(calib.digP4) << 35;
  pVar1 = ((pVar1 * pVar1 * static_cast<int64_t>(calib.digP3)) >> 8) + ((pVar1 * static_cast<int64_t>(calib.digP2)) << 12);
  pVar1 = (((static_cast<int64_t>(1) << 47) + pVar1) * static_cast<int64_t>(calib.digP1)) >> 33;
  if (pVar1 == 0) return false;
  int64_t pressure = 1048576 - adcP;
  pressure = (((pressure << 31) - pVar2) * 3125) / pVar1;
  pVar1 = (static_cast<int64_t>(calib.digP9) * (pressure >> 13) * (pressure >> 13)) >> 25;
  pVar2 = (static_cast<int64_t>(calib.digP8) * pressure) >> 19;
  pressure = ((pressure + pVar1 + pVar2) >> 8) + (static_cast<int64_t>(calib.digP7) << 4);
  pressurePa = static_cast<float>(pressure) / 256.0f;
  return pressurePa > 0.0f;
}

bool readConfiguredBmp280(float &tempC, float &pressurePa, uint8_t &addrOut) {
  String tokens[8];
  size_t count = 0;
  splitCsv(g_cfg.bmp280Addrs, tokens, 8, count);
  for (size_t i = 0; i < count; ++i) {
    const uint8_t addr = static_cast<uint8_t>(strtoul(tokens[i].c_str(), nullptr, 16));
    if (readBmp280Addr(addr, tempC, pressurePa)) {
      addrOut = addr;
      return true;
    }
  }
  return false;
}

String mqttReadSummary() {
  if (!isFresh(g_mqtt.lastRxMs)) {
    return "@MQTT=RX:TOPIC=-,NAME=" + sanitizeResponseValue(g_cfg.mqttName, 24) + ",PAYLOAD=-!";
  }
  return buildMqttRxLine();
}

void mqttStoreRawSample(const String &topic, const String &payload, uint32_t sampleMs) {
  if (topic.isEmpty()) return;

  auto isConfiguredProtectedTopic = [&](const String &candidateTopic) {
    String configured[16];
    size_t count = 0;
    if (!parseTopicList(g_cfg.mqttTopics, configured, 16, count)) return false;

    const String candidate = normalizeTopicPath(candidateTopic);
    for (size_t i = 0; i < count; ++i) {
      const String cfgTopic = normalizeTopicPath(configured[i]);
      if (cfgTopic.isEmpty()) continue;
      if (candidate == cfgTopic || candidate.startsWith(cfgTopic + "/")) {
        return true;
      }
    }
    return false;
  };

  const uint32_t newSeq = ++g_mqtt.rawSeq;
  bool existingTopic = false;

  int idx = -1;
  for (size_t i = 0; i < kMqttRawCacheSlots; ++i) {
    if (g_mqtt.raw[i].used && topic == g_mqtt.raw[i].topic) {
      idx = static_cast<int>(i);
      existingTopic = true;
      break;
    }
  }

  if (idx < 0) {
    for (size_t i = 0; i < kMqttRawCacheSlots; ++i) {
      if (!g_mqtt.raw[i].used) {
        idx = static_cast<int>(i);
        break;
      }
    }
  }

  if (idx < 0) {
    size_t oldest = static_cast<size_t>(-1);
    for (size_t i = 0; i < kMqttRawCacheSlots; ++i) {
      if (!g_mqtt.raw[i].used) continue;
      if (isConfiguredProtectedTopic(g_mqtt.raw[i].topic)) continue;
      if (oldest == static_cast<size_t>(-1) || g_mqtt.raw[i].sampleMs < g_mqtt.raw[oldest].sampleMs) {
        oldest = i;
      }
    }

    if (oldest == static_cast<size_t>(-1)) {
      oldest = 0;
      for (size_t i = 1; i < kMqttRawCacheSlots; ++i) {
        if (g_mqtt.raw[i].sampleMs < g_mqtt.raw[oldest].sampleMs) {
          oldest = i;
        }
      }
    }

    idx = static_cast<int>(oldest);
  }

  g_mqtt.raw[idx].used = true;
  strlcpy(g_mqtt.raw[idx].topic, topic.c_str(), sizeof(g_mqtt.raw[idx].topic));
  strlcpy(g_mqtt.raw[idx].payload, payload.c_str(), sizeof(g_mqtt.raw[idx].payload));
  g_mqtt.raw[idx].sampleMs = sampleMs;
  g_mqtt.raw[idx].seq = newSeq;
  if (!existingTopic) {
    g_mqtt.raw[idx].firstSeq = newSeq;
  }
}

bool mqttFindRawLatest(String &topicOut, String &payloadOut) {
  bool found = false;
  uint32_t bestSeq = 0;
  size_t bestIdx = 0;
  for (size_t i = 0; i < kMqttRawCacheSlots; ++i) {
    if (!g_mqtt.raw[i].used) continue;
    if (!found || g_mqtt.raw[i].seq > bestSeq) {
      found = true;
      bestSeq = g_mqtt.raw[i].seq;
      bestIdx = i;
    }
  }
  if (!found) return false;

  topicOut = g_mqtt.raw[bestIdx].topic;
  payloadOut = g_mqtt.raw[bestIdx].payload;
  return true;
}

bool mqttFindRawFirst(String &topicOut, String &payloadOut) {
  bool found = false;
  uint32_t bestFirstSeq = 0;
  size_t bestIdx = 0;
  for (size_t i = 0; i < kMqttRawCacheSlots; ++i) {
    if (!g_mqtt.raw[i].used) continue;
    if (!found || g_mqtt.raw[i].firstSeq < bestFirstSeq) {
      found = true;
      bestFirstSeq = g_mqtt.raw[i].firstSeq;
      bestIdx = i;
    }
  }
  if (!found) return false;

  topicOut = g_mqtt.raw[bestIdx].topic;
  payloadOut = g_mqtt.raw[bestIdx].payload;
  return true;
}

bool mqttPayloadLooksArray(const String &payload) {
  String trimmed = payload;
  trimmed.trim();
  return trimmed.startsWith("[") && trimmed.endsWith("]");
}

String mqttCanonicalTopic(const String &topic, const String &payload) {
  (void)payload;
  return normalizeTopicPath(topic);
}

bool mqttTopicEquals(const String &lhs, const String &rhs) {
  return normalizeTopicPath(lhs) == normalizeTopicPath(rhs);
}

int mqttTopicDistance(const String &lhs, const String &rhs) {
  const String left = normalizeTopicPath(lhs);
  const String right = normalizeTopicPath(rhs);
  if (left.isEmpty() || right.isEmpty()) return INT_MAX;
  if (left == right) return 0;

  if (right.startsWith(left) && right.length() > left.length() && right[left.length()] == '/') {
    int distance = 0;
    for (size_t i = left.length() + 1; i < right.length(); ++i) {
      if (right[i] == '/') distance++;
    }
    return distance + 1;
  }

  if (left.startsWith(right) && left.length() > right.length() && left[right.length()] == '/') {
    int distance = 0;
    for (size_t i = right.length() + 1; i < left.length(); ++i) {
      if (left[i] == '/') distance++;
    }
    return distance + 1;
  }

  return INT_MAX;
}

String mqttFormatPayloadForRead(const String &topic, const String &payload) {
  return sanitizeResponseValue(payload, 720);
}

bool mqttTopicMatchesRequest(const String &requestTopic, const String &cachedTopic) {
  String req = normalizeTopicPath(requestTopic);
  String cached = normalizeTopicPath(cachedTopic);

  if (req.isEmpty() || cached.isEmpty()) return false;
  if (req == cached) return true;

  if (cached.startsWith(req) && cached.length() > req.length() && cached[req.length()] == '/') {
    return true;
  }
  if (req.startsWith(cached) && req.length() > cached.length() && req[cached.length()] == '/') {
    return true;
  }
  return false;
}

bool mqttTryTopicSuffix(const String &prefixTopic, const String &fullTopic, String &suffixOut) {
  const String prefix = normalizeTopicPath(prefixTopic);
  const String full = normalizeTopicPath(fullTopic);
  if (prefix.isEmpty() || full.isEmpty()) return false;

  if (prefix == full) {
    suffixOut = "";
    return true;
  }

  const String withSlash = prefix + "/";
  if (!full.startsWith(withSlash)) return false;

  suffixOut = full.substring(withSlash.length());
  return !suffixOut.isEmpty();
}

void mqttSplitTopicPath(const String &path, String *out, size_t cap, size_t &count) {
  count = 0;
  if (cap == 0) return;
  int start = 0;
  while (start < path.length() && count < cap) {
    const int slash = path.indexOf('/', start);
    String token = slash >= 0 ? path.substring(start, slash) : path.substring(start);
    token.trim();
    if (!token.isEmpty()) out[count++] = token;
    if (slash < 0) break;
    start = slash + 1;
  }
}

String mqttJsonKeyEscape(const String &key) {
  String out;
  out.reserve(key.length());
  for (size_t i = 0; i < key.length(); ++i) {
    const char ch = key[i];
    if (ch == '\\' || ch == '"') out += '\\';
    out += ch;
  }
  return out;
}

String mqttWrapPayloadForParentTopic(const String &childSuffix, const String &leafPayloadJson) {
  String segments[12];
  size_t count = 0;
  mqttSplitTopicPath(childSuffix, segments, 12, count);
  if (count == 0) return leafPayloadJson;

  String wrapped = leafPayloadJson;
  for (size_t i = count; i > 0; --i) {
    wrapped = "{\"" + mqttJsonKeyEscape(segments[i - 1]) + "\":" + wrapped + "}";
  }
  return sanitizeResponseValue(wrapped, 720);
}

bool mqttExtractPayloadByPath(const String &payloadJson, const String &subPath, String &valueOut) {
  if (subPath.isEmpty()) return false;

  String segments[12];
  size_t count = 0;
  mqttSplitTopicPath(subPath, segments, 12, count);
  if (count == 0) return false;

  JsonDocument doc;
  const DeserializationError err = deserializeJson(doc, payloadJson);
  if (err) return false;

  JsonVariantConst current = doc.as<JsonVariantConst>();
  for (size_t i = 0; i < count; ++i) {
    const String &segment = segments[i];

    if (current.is<JsonObjectConst>()) {
      JsonObjectConst obj = current.as<JsonObjectConst>();
      bool found = false;
      for (JsonPairConst kv : obj) {
        String key = kv.key().c_str();
        if (key.equalsIgnoreCase(segment)) {
          current = kv.value();
          found = true;
          break;
        }
      }
      if (!found) {
        String alias = segment;
        if (segment.equalsIgnoreCase("diagnostics") || segment.equalsIgnoreCase("diagnostic")) {
          alias = "diag";
        }
        if (!alias.equalsIgnoreCase(segment)) {
          for (JsonPairConst kv : obj) {
            String key = kv.key().c_str();
            if (key.equalsIgnoreCase(alias)) {
              current = kv.value();
              found = true;
              break;
            }
          }
        }
      }
      if (!found) return false;
      continue;
    }

    if (current.is<JsonArrayConst>()) {
      bool numeric = !segment.isEmpty();
      for (size_t j = 0; j < segment.length(); ++j) {
        if (!isdigit(static_cast<unsigned char>(segment[j]))) {
          numeric = false;
          break;
        }
      }
      if (!numeric) return false;

      const int index = segment.toInt();
      JsonArrayConst arr = current.as<JsonArrayConst>();
      if (index < 0 || static_cast<size_t>(index) >= arr.size()) return false;
      current = arr[static_cast<size_t>(index)];
      continue;
    }

    return false;
  }

  valueOut = mqttJsonLiteral(current, 720);
  return true;
}

bool mqttResolveConfiguredTopicPayload(const String &configuredTopic,
                                      const String &incomingTopic,
                                      const String &incomingPayload,
                                      String &resolvedPayloadOut) {
  const String configured = normalizeTopicPath(configuredTopic);
  const String incoming = normalizeTopicPath(incomingTopic);
  if (configured.isEmpty() || incoming.isEmpty()) return false;

  if (mqttConfiguredPathMatchesTopic(configured, incoming)) {
    resolvedPayloadOut = incomingPayload;
    return true;
  }

  String nestedSuffix;
  if (!mqttTryTopicSuffix(incoming, configured, nestedSuffix)) {
    return false;
  }

  return mqttExtractPayloadByPath(incomingPayload, nestedSuffix, resolvedPayloadOut);
}

void mqttResolveConfiguredTopicPayloads(const String &listCsv,
                                       const String &incomingTopic,
                                       const String &incomingPayload,
                                       String *topicsOut,
                                       String *payloadsOut,
                                       size_t cap,
                                       size_t &count) {
  count = 0;
  if (cap == 0) return;

  String configured[16];
  size_t configuredCount = 0;
  if (!parseTopicList(listCsv, configured, 16, configuredCount)) {
    return;
  }

  for (size_t i = 0; i < configuredCount && count < cap; ++i) {
    String resolvedPayload;
    if (!mqttResolveConfiguredTopicPayload(configured[i], incomingTopic, incomingPayload, resolvedPayload)) {
      continue;
    }

    const String normalizedTopic = normalizeTopicPath(configured[i]);
    bool seen = false;
    for (size_t j = 0; j < count; ++j) {
      if (mqttTopicEquals(topicsOut[j], normalizedTopic)) {
        payloadsOut[j] = resolvedPayload;
        seen = true;
        break;
      }
    }
    if (seen) continue;

    topicsOut[count] = normalizedTopic;
    payloadsOut[count] = resolvedPayload;
    count++;
  }
}

bool mqttFindExactRawTopic(const String &topic, String &payloadOut) {
  for (size_t i = 0; i < kMqttRawCacheSlots; ++i) {
    if (!g_mqtt.raw[i].used) continue;
    if (mqttTopicEquals(topic, mqttCanonicalTopic(g_mqtt.raw[i].topic, g_mqtt.raw[i].payload))) {
      payloadOut = mqttFormatPayloadForRead(g_mqtt.raw[i].topic, g_mqtt.raw[i].payload);
      return true;
    }
  }
  return false;
}

bool mqttCandidateBetter(bool foundCurrent,
                         bool candidateExactRequest,
                         int candidateRequestDistance,
                         bool candidateExactConfigured,
                         int candidateConfiguredDistance,
                         uint32_t candidateFirstSeq,
                         bool bestExactRequest,
                         int bestRequestDistance,
                         bool bestExactConfigured,
                         int bestConfiguredDistance,
                         uint32_t bestFirstSeq) {
  if (!foundCurrent) return true;
  if (candidateExactRequest != bestExactRequest) return candidateExactRequest;
  if (candidateRequestDistance != bestRequestDistance) return candidateRequestDistance < bestRequestDistance;
  if (candidateExactConfigured != bestExactConfigured) return candidateExactConfigured;
  if (candidateConfiguredDistance != bestConfiguredDistance) return candidateConfiguredDistance < bestConfiguredDistance;
  return candidateFirstSeq < bestFirstSeq;
}

bool mqttTopicCandidateBetter(bool foundCurrent,
                              bool candidateExactRequest,
                              int candidateRequestDistance,
                              uint32_t candidateFirstSeq,
                              bool bestExactRequest,
                              int bestRequestDistance,
                              uint32_t bestFirstSeq) {
  if (!foundCurrent) return true;
  if (candidateExactRequest != bestExactRequest) return candidateExactRequest;
  if (candidateRequestDistance != bestRequestDistance) return candidateRequestDistance < bestRequestDistance;
  return candidateFirstSeq < bestFirstSeq;
}

bool mqttTopicCandidateBetterFresh(bool foundCurrent,
                                   bool candidateExactRequest,
                                   int candidateRequestDistance,
                                   uint32_t candidateSeq,
                                   bool bestExactRequest,
                                   int bestRequestDistance,
                                   uint32_t bestSeq) {
  if (!foundCurrent) return true;
  if (candidateExactRequest != bestExactRequest) return candidateExactRequest;
  if (candidateRequestDistance != bestRequestDistance) return candidateRequestDistance < bestRequestDistance;
  return candidateSeq > bestSeq;
}

bool mqttTopicHasWildcard(const String &topic) {
  return topic.indexOf('#') >= 0 || topic.indexOf('+') >= 0;
}

bool mqttTopicMatchesSubscriptionFilter(const String &subscriptionTopic, const String &actualTopic) {
  String sub = normalizeTopicPath(subscriptionTopic);
  String actual = normalizeTopicPath(actualTopic);
  if (sub.isEmpty() || actual.isEmpty()) return false;

  // Minimal MQTT wildcard handling needed by current config patterns.
  if (sub.endsWith("/#")) {
    const String prefix = sub.substring(0, sub.length() - 2);
    return actual == prefix || (actual.startsWith(prefix + "/"));
  }

  if (sub.endsWith("/+") || sub == "+") {
    const int slash = sub.lastIndexOf('/');
    const String prefix = slash >= 0 ? sub.substring(0, slash) : String("");
    if (!prefix.isEmpty()) {
      if (!actual.startsWith(prefix + "/")) return false;
      const String suffix = actual.substring(prefix.length() + 1);
      return suffix.indexOf('/') < 0;
    }
    return actual.indexOf('/') < 0;
  }

  return actual == sub || actual.startsWith(sub + "/");
}

bool mqttTopicAllowedForSummary(const String &topic, const String &payload) {
  const String canonical = mqttCanonicalTopic(topic, payload);
  return mqttConfiguredListMatchesTopic(g_cfg.mqttTopics, canonical) || mqttConfiguredListMatchesTopic(g_cfg.mqttTopics, topic);
}

bool mqttPathStartsWith(const String &path, const String &prefix) {
  const String p = normalizeTopicPath(path);
  const String pre = normalizeTopicPath(prefix);
  if (p.isEmpty() || pre.isEmpty()) return false;
  return p == pre || p.startsWith(pre + "/");
}

bool mqttIsTopicInList(const String &listCsv, const String &topic) {
  String configured[16];
  size_t count = 0;
  if (!parseTopicList(listCsv, configured, 16, count)) return false;

  const String candidate = normalizeTopicPath(topic);
  for (size_t i = 0; i < count; ++i) {
    const String configuredTopic = normalizeTopicPath(configured[i]);
    if (mqttTopicHasWildcard(configuredTopic)) {
      if (mqttTopicMatchesSubscriptionFilter(configuredTopic, candidate)) {
        return true;
      }
      continue;
    }
    if (mqttPathStartsWith(candidate, configuredTopic)) {
      return true;
    }
  }

  return false;
}

String mqttValueToText(const JsonVariantConst &value, size_t maxLen = 120) {
  if (value.is<bool>()) return value.as<bool>() ? "1" : "0";
  if (value.is<long long>()) return String(value.as<long long>());
  if (value.is<unsigned long long>()) return String(static_cast<unsigned long>(value.as<unsigned long long>()));
  if (value.is<float>() || value.is<double>()) return String(value.as<double>(), 6);
  if (value.is<const char *>()) return sanitizeResponseValue(String(value.as<const char *>()), maxLen);
  if (value.is<String>()) return sanitizeResponseValue(value.as<String>(), maxLen);
  return mqttJsonLiteral(value, maxLen);
}

bool mqttFindObjectKeyIgnoreCase(JsonObjectConst obj, const String &segment, String &resolvedKey) {
  JsonVariantConst direct = obj[segment];
  if (!direct.isNull()) {
    resolvedKey = segment;
    return true;
  }

  for (JsonPairConst kv : obj) {
    const String key = kv.key().c_str();
    if (key.equalsIgnoreCase(segment)) {
      resolvedKey = key;
      return true;
    }
  }
  return false;
}

void mqttMergeCacheValue(JsonVariant dst, const JsonVariantConst &src);

bool mqttJsonFindPath(const JsonVariantConst &root, const String &path, JsonVariantConst &valueOut) {
  JsonVariantConst current = root;
  if (path.isEmpty()) {
    valueOut = current;
    return true;
  }

  String segments[20];
  size_t count = 0;
  mqttSplitTopicPath(normalizeTopicPath(path), segments, 20, count);
  if (count == 0) {
    valueOut = current;
    return true;
  }

  for (size_t i = 0; i < count; ++i) {
    const String &segment = segments[i];
    if (current.is<JsonObjectConst>()) {
      JsonObjectConst obj = current.as<JsonObjectConst>();
      String resolved;
      if (!mqttFindObjectKeyIgnoreCase(obj, segment, resolved)) return false;
      current = obj[resolved];
      if (current.isNull()) return false;
      continue;
    }

    if (current.is<JsonArrayConst>()) {
      bool numeric = !segment.isEmpty();
      for (size_t j = 0; j < segment.length(); ++j) {
        if (!isdigit(static_cast<unsigned char>(segment[j]))) {
          numeric = false;
          break;
        }
      }
      if (!numeric) return false;

      const int index = segment.toInt();
      JsonArrayConst arr = current.as<JsonArrayConst>();
      if (index < 0 || static_cast<size_t>(index) >= arr.size()) return false;
      current = arr[static_cast<size_t>(index)];
      continue;
    }

    return false;
  }

  valueOut = current;
  return true;
}

void mqttInsertVariantAtPath(JsonObject root, const String &path, const JsonVariantConst &value) {
  if (path.isEmpty()) {
    mqttMergeCacheValue(root, value);
    return;
  }

  String segments[20];
  size_t count = 0;
  mqttSplitTopicPath(path, segments, 20, count);
  if (count == 0) {
    mqttMergeCacheValue(root, value);
    return;
  }

  JsonObject cursor = root;
  for (size_t i = 0; i + 1 < count; ++i) {
    JsonVariant child = cursor[segments[i]];
    if (!child.is<JsonObject>()) {
      cursor.remove(segments[i]);
      cursor[segments[i]].to<JsonObject>();
    }
    cursor = cursor[segments[i]].as<JsonObject>();
  }

  mqttMergeCacheValue(cursor[segments[count - 1]], value);
}

bool mqttCacheFindPath(const String &topicPath, JsonVariantConst &valueOut) {
  if (!g_mqtt.cacheDoc.is<JsonObjectConst>()) return false;

  String segments[20];
  size_t count = 0;
  mqttSplitTopicPath(normalizeTopicPath(topicPath), segments, 20, count);

  JsonVariantConst current = g_mqtt.cacheDoc.as<JsonVariantConst>();
  if (count == 0) {
    valueOut = current;
    return true;
  }

  for (size_t i = 0; i < count; ++i) {
    if (!current.is<JsonObjectConst>()) return false;
    JsonObjectConst obj = current.as<JsonObjectConst>();
    String resolved;
    if (!mqttFindObjectKeyIgnoreCase(obj, segments[i], resolved)) return false;
    current = obj[resolved];
    if (current.isNull()) return false;
  }

  valueOut = current;
  return true;
}

void mqttMergeCacheValue(JsonVariant dst, const JsonVariantConst &src) {
  if (src.is<JsonObjectConst>()) {
    if (!dst.is<JsonObject>()) {
      dst.clear();
      dst.to<JsonObject>();
    }

    JsonObject dstObj = dst.as<JsonObject>();
    JsonObjectConst srcObj = src.as<JsonObjectConst>();
    for (JsonPairConst kv : srcObj) {
      mqttMergeCacheValue(dstObj[kv.key().c_str()], kv.value());
    }
    return;
  }

  if (src.is<JsonArrayConst>()) {
    dst.set(src);
    return;
  }

  dst.set(src);
}

void mqttCacheStoreTopicPayload(const String &topic, const String &payload) {
  String normalizedTopic = normalizeTopicPath(topic);
  if (!mqttConfiguredListMatchesTopic(g_cfg.mqttCacheTopics, normalizedTopic)) {
    return;
  }

  String segments[20];
  size_t count = 0;
  mqttSplitTopicPath(normalizedTopic, segments, 20, count);
  if (count == 0) return;

  String normalizedPayload = payload;

  JsonDocument payloadDoc;
  const DeserializationError payloadErr = deserializeJson(payloadDoc, normalizedPayload);

  JsonObject root;
  if (g_mqtt.cacheDoc.is<JsonObject>()) {
    root = g_mqtt.cacheDoc.as<JsonObject>();
  } else {
    root = g_mqtt.cacheDoc.to<JsonObject>();
  }
  JsonObject cursor = root;
  for (size_t i = 0; i + 1 < count; ++i) {
    JsonVariant child = cursor[segments[i]];
    if (!child.is<JsonObject>()) {
      cursor.remove(segments[i]);
      cursor[segments[i]].to<JsonObject>();
    }
    cursor = cursor[segments[i]].as<JsonObject>();
  }

  const String leaf = segments[count - 1];
  if (!payloadErr) {
    cursor[leaf].set(payloadDoc.as<JsonVariantConst>());
  } else {
    cursor[leaf] = sanitizeResponseValue(normalizedPayload, 720);
  }

  g_mqtt.cacheRev++;
}

void mqttAppendFlattenedCacheFields(const JsonVariantConst &value, const String &path, String &out) {
  if (value.is<JsonObjectConst>()) {
    JsonObjectConst obj = value.as<JsonObjectConst>();
    for (JsonPairConst kv : obj) {
      String nextPath = path;
      if (!nextPath.isEmpty()) nextPath += '_';
      nextPath += kv.key().c_str();
      mqttAppendFlattenedCacheFields(kv.value(), nextPath, out);
    }
    return;
  }

  if (value.is<JsonArrayConst>()) {
    JsonArrayConst arr = value.as<JsonArrayConst>();
    size_t idx = 0;
    for (JsonVariantConst item : arr) {
      String nextPath = path;
      if (!nextPath.isEmpty()) nextPath += '_';
      nextPath += String(idx);
      mqttAppendFlattenedCacheFields(item, nextPath, out);
      idx++;
    }
    return;
  }

  String keyPath = sanitizeMqttKey(path);
  if (keyPath.isEmpty()) keyPath = "VALUE";

  out += ',';
  out += mqttNamePrefix();
  out += '_';
  out += asciiLower(keyPath);
  out += '=';
  out += mqttValueToText(value, 160);
}

bool mqttBuildRawCacheView(const String &requestedTopic,
                          JsonDocument &viewDoc,
                          bool &hasScalarOut,
                          String &scalarOut) {
  const String requested = normalizeTopicPath(requestedTopic);
  if (requested.isEmpty()) return false;

  JsonObject root = viewDoc.to<JsonObject>();
  bool found = false;
  hasScalarOut = false;
  scalarOut = "";

  for (size_t i = 0; i < kMqttRawCacheSlots; ++i) {
    if (!g_mqtt.raw[i].used) continue;

    const String canonicalTopic = mqttCanonicalTopic(g_mqtt.raw[i].topic, g_mqtt.raw[i].payload);
    if (!mqttConfiguredListMatchesTopic(g_cfg.mqttCacheTopics, canonicalTopic)) continue;
    const String formattedPayload = mqttFormatPayloadForRead(g_mqtt.raw[i].topic, g_mqtt.raw[i].payload);

    JsonDocument payloadDoc;
    const DeserializationError payloadErr = deserializeJson(payloadDoc, formattedPayload);

    if (mqttTopicEquals(requested, canonicalTopic)) {
      found = true;
      if (!payloadErr) {
        JsonVariantConst payloadVariant = payloadDoc.as<JsonVariantConst>();
        if (payloadVariant.is<JsonObjectConst>()) {
          mqttMergeCacheValue(root, payloadVariant);
        } else {
          hasScalarOut = true;
          scalarOut = mqttValueToText(payloadVariant, 720);
        }
      } else {
        hasScalarOut = true;
        scalarOut = sanitizeResponseValue(formattedPayload, 720);
      }
      continue;
    }

    String nestedSuffix;
    if (mqttTryTopicSuffix(canonicalTopic, requested, nestedSuffix)) {
      if (payloadErr) continue;

      JsonVariantConst nestedValue;
      if (!mqttJsonFindPath(payloadDoc.as<JsonVariantConst>(), nestedSuffix, nestedValue)) {
        continue;
      }

      found = true;
      if (nestedValue.is<JsonObjectConst>()) {
        mqttMergeCacheValue(root, nestedValue);
      } else {
        hasScalarOut = true;
        scalarOut = mqttValueToText(nestedValue, 720);
      }
      continue;
    }

    String descendantSuffix;
    if (!mqttTryTopicSuffix(requested, canonicalTopic, descendantSuffix)) {
      continue;
    }

    found = true;
    if (!payloadErr) {
      mqttInsertVariantAtPath(root, descendantSuffix, payloadDoc.as<JsonVariantConst>());
    } else {
      String segments[20];
      size_t count = 0;
      mqttSplitTopicPath(descendantSuffix, segments, 20, count);
      if (count == 0) continue;

      JsonObject cursor = root;
      for (size_t seg = 0; seg + 1 < count; ++seg) {
        JsonVariant child = cursor[segments[seg]];
        if (!child.is<JsonObject>()) {
          cursor.remove(segments[seg]);
          cursor[segments[seg]].to<JsonObject>();
        }
        cursor = cursor[segments[seg]].as<JsonObject>();
      }
      cursor[segments[count - 1]] = sanitizeResponseValue(formattedPayload, 720);
    }
  }

  return found;
}

String mqttPollFieldsFromCache() {
  if (!isFresh(g_mqtt.lastRxMs)) {
    return "";
  }

  String topics[16];
  size_t count = 0;
  if (!parseTopicList(g_cfg.mqttTopics, topics, 16, count) || count == 0) {
    return "";
  }

  String out;
  for (size_t i = 0; i < count; ++i) {
    JsonDocument topicDoc;
    bool hasScalar = false;
    String scalarValue;
    if (!mqttBuildRawCacheView(topics[i], topicDoc, hasScalar, scalarValue)) {
      continue;
    }

    JsonObjectConst topicRoot = topicDoc.as<JsonObjectConst>();
    if (topicRoot.size() > 0) {
      mqttAppendFlattenedCacheFields(topicDoc.as<JsonVariantConst>(), "", out);
    } else if (hasScalar) {
      String keyPath = sanitizeMqttKey(mqttTopicLeaf(topics[i]));
      if (keyPath.isEmpty()) keyPath = "VALUE";
      out += ',';
      out += mqttNamePrefix();
      out += '_';
      out += asciiLower(keyPath);
      out += '=';
      out += scalarValue;
    }
  }

  if (out.isEmpty()) {
    return "";
  }
  return out;
}

bool mqttReadRawTopicFromCache(const String &requestedTopic, String &payloadOut) {
  JsonDocument viewDoc;
  bool hasScalar = false;
  String scalarValue;
  if (!mqttBuildRawCacheView(requestedTopic, viewDoc, hasScalar, scalarValue)) {
    return false;
  }

  JsonObjectConst root = viewDoc.as<JsonObjectConst>();
  if (root.size() > 0) {
    payloadOut = mqttJsonLiteral(viewDoc.as<JsonVariantConst>(), 720);
    return true;
  }

  if (hasScalar) {
    payloadOut = scalarValue;
    return true;
  }

  return false;
}

bool mqttIsConfiguredSubscription(const String &subscriptionTopic) {
  const String candidate = normalizeTopicPath(subscriptionTopic);
  String configured[16];
  size_t count = 0;
  if (!parseTopicList(g_cfg.mqttTopics, configured, 16, count)) {
    return false;
  }

  for (size_t i = 0; i < count; ++i) {
    const String configuredTopic = normalizeTopicPath(configured[i]);
    if (candidate == configuredTopic) {
      return true;
    }

    if (!mqttTopicHasWildcard(configuredTopic)) {
      const String descendants = configuredTopic.endsWith("/") ? configuredTopic + "#" : configuredTopic + "/#";
      if (candidate == descendants) {
        return true;
      }
    }
  }

  return false;
}

bool mqttFindFreshRawTopic(const String &topic,
                          uint32_t minSeqExclusive,
                          String &matchedTopicOut,
                          String &payloadOut) {
  bool found = false;
  bool bestExactRequest = false;
  int bestRequestDistance = INT_MAX;
  uint32_t bestSeq = 0;

  for (size_t i = 0; i < kMqttRawCacheSlots; ++i) {
    if (!g_mqtt.raw[i].used) continue;
    if (g_mqtt.raw[i].seq <= minSeqExclusive) continue;

    const String canonicalTopic = mqttCanonicalTopic(g_mqtt.raw[i].topic, g_mqtt.raw[i].payload);
    if (!mqttTopicMatchesRequest(topic, canonicalTopic)) continue;

    const bool exactRequest = mqttTopicEquals(topic, canonicalTopic);
    const int requestDistance = mqttTopicDistance(topic, canonicalTopic);
    if (mqttTopicCandidateBetterFresh(found,
                                      exactRequest,
                                      requestDistance,
                                      g_mqtt.raw[i].seq,
                                      bestExactRequest,
                                      bestRequestDistance,
                                      bestSeq)) {
      bestExactRequest = exactRequest;
      bestRequestDistance = requestDistance;
      bestSeq = g_mqtt.raw[i].seq;
      matchedTopicOut = canonicalTopic;
      payloadOut = mqttFormatPayloadForRead(g_mqtt.raw[i].topic, g_mqtt.raw[i].payload);
      found = true;
    }
  }

  return found;
}

bool mqttReadTopicOnDemand(const String &topic, String &matchedTopicOut, String &payloadOut) {
  if (!g_cfg.mqttEnabled || !g_mqtt.connected || topic.isEmpty()) return false;
  if (!mqttIsTopicInList(g_cfg.mqttCacheTopics, topic)) return false;

  const uint32_t startSeq = g_mqtt.rawSeq;
  bool subscribedExact = false;
  bool subscribedDesc = false;
  const bool exactIsConfigured = mqttIsConfiguredSubscription(topic);

  if (MqttClient.subscribe(topic.c_str())) {
    subscribedExact = true;
  }

  String descendants;
  bool descIsConfigured = false;
  if (!mqttTopicHasWildcard(topic)) {
    descendants = topic.endsWith("/") ? topic + "#" : topic + "/#";
    descIsConfigured = mqttIsConfiguredSubscription(descendants);
    if (MqttClient.subscribe(descendants.c_str())) {
      subscribedDesc = true;
    }
  }

  const uint32_t startMs = millis();
  while (millis() - startMs < kMqttReadWaitMs) {
    if (!MqttClient.connected()) break;
    MqttClient.loop();
    if (mqttFindFreshRawTopic(topic, startSeq, matchedTopicOut, payloadOut)) {
      if (subscribedExact && !exactIsConfigured) {
        MqttClient.unsubscribe(topic.c_str());
      }
      if (subscribedDesc && !descIsConfigured) {
        MqttClient.unsubscribe(descendants.c_str());
      }
      return true;
    }
    delay(10);
  }

  if (subscribedExact && !exactIsConfigured) {
    MqttClient.unsubscribe(topic.c_str());
  }
  if (subscribedDesc && !descIsConfigured) {
    MqttClient.unsubscribe(descendants.c_str());
  }

  return false;
}

bool mqttFindConfiguredPreferredRaw(const String &requestTopic,
                                    String &matchedTopicOut,
                                    String &payloadOut,
                                    String *rawPayloadOut = nullptr) {
  String configured[16];
  size_t count = 0;
  if (!parseTopicList(g_cfg.mqttTopics, configured, 16, count) || count == 0) {
    return false;
  }

  bool found = false;
  bool bestExactRequest = false;
  int bestRequestDistance = INT_MAX;
  bool bestExactConfigured = false;
  int bestConfiguredDistance = INT_MAX;
  uint32_t bestFirstSeq = 0;
  size_t bestIdx = 0;
  String bestCanonicalTopic;

  for (size_t i = 0; i < count; ++i) {
    for (size_t j = 0; j < kMqttRawCacheSlots; ++j) {
      if (!g_mqtt.raw[j].used) continue;
      const String canonicalTopic = mqttCanonicalTopic(g_mqtt.raw[j].topic, g_mqtt.raw[j].payload);
      if (!mqttTopicMatchesRequest(configured[i], canonicalTopic)) continue;
      if (!requestTopic.isEmpty() && !mqttTopicMatchesRequest(requestTopic, canonicalTopic)) continue;

      const bool exactRequest = !requestTopic.isEmpty() && mqttTopicEquals(requestTopic, canonicalTopic);
      const int requestDistance = requestTopic.isEmpty() ? INT_MAX : mqttTopicDistance(requestTopic, canonicalTopic);
      const bool exactConfigured = mqttTopicEquals(configured[i], canonicalTopic);
      const int configuredDistance = mqttTopicDistance(configured[i], canonicalTopic);

      if (mqttCandidateBetter(found,
                              exactRequest,
                              requestDistance,
                              exactConfigured,
                              configuredDistance,
                              g_mqtt.raw[j].firstSeq,
                              bestExactRequest,
                              bestRequestDistance,
                              bestExactConfigured,
                              bestConfiguredDistance,
                              bestFirstSeq)) {
        found = true;
        bestIdx = j;
        bestCanonicalTopic = canonicalTopic;
        bestExactRequest = exactRequest;
        bestRequestDistance = requestDistance;
        bestExactConfigured = exactConfigured;
        bestConfiguredDistance = configuredDistance;
        bestFirstSeq = g_mqtt.raw[j].firstSeq;
      }
    }
  }

  if (!found) return false;

  matchedTopicOut = bestCanonicalTopic;
  payloadOut = mqttFormatPayloadForRead(g_mqtt.raw[bestIdx].topic, g_mqtt.raw[bestIdx].payload);
  if (rawPayloadOut) {
    *rawPayloadOut = String(g_mqtt.raw[bestIdx].payload);
  }
  return true;
}

bool mqttFindRawTopic(const String &topic,
                      String &matchedTopicOut,
                      String &payloadOut,
                      String *rawPayloadOut = nullptr) {
  if (mqttFindConfiguredPreferredRaw(topic, matchedTopicOut, payloadOut, rawPayloadOut)) {
    return true;
  }

  bool found = false;
  bool bestExactRequest = false;
  int bestRequestDistance = INT_MAX;
  uint32_t bestFirstSeq = 0;
  for (size_t i = 0; i < kMqttRawCacheSlots; ++i) {
    if (!g_mqtt.raw[i].used) continue;
    const String canonicalTopic = mqttCanonicalTopic(g_mqtt.raw[i].topic, g_mqtt.raw[i].payload);
    if (!mqttTopicMatchesRequest(topic, canonicalTopic)) continue;

    const bool exactRequest = mqttTopicEquals(topic, canonicalTopic);
    const int requestDistance = mqttTopicDistance(topic, canonicalTopic);
    if (mqttTopicCandidateBetter(found,
                                 exactRequest,
                                 requestDistance,
                                 g_mqtt.raw[i].firstSeq,
                                 bestExactRequest,
                                 bestRequestDistance,
                                 bestFirstSeq)) {
      bestExactRequest = exactRequest;
      bestRequestDistance = requestDistance;
      bestFirstSeq = g_mqtt.raw[i].firstSeq;
      matchedTopicOut = canonicalTopic;
      payloadOut = mqttFormatPayloadForRead(g_mqtt.raw[i].topic, g_mqtt.raw[i].payload);
      if (rawPayloadOut) {
        *rawPayloadOut = String(g_mqtt.raw[i].payload);
      }
      found = true;
    }
  }
  return found;
}

String mqttReadRawTopic(const String &topic) {
  const String requestedTopic = normalizeTopicPath(topic);
  if (!mqttIsTopicInList(g_cfg.mqttCacheTopics, requestedTopic)) {
    return "@MQTT=ERR:NO_TOPIC_DATA!";
  }

  String payload;
  if (!mqttReadRawTopicFromCache(requestedTopic, payload)) {
    return "@MQTT=ERR:NO_TOPIC_DATA!";
  }

  return "@MQTT=RX:TOPIC=" + sanitizeResponseValue(requestedTopic, 96) + ",NAME=" + sanitizeResponseValue(g_cfg.mqttName, 24) + ",JSON=" + sanitizeResponseValue(payload, 720) + "!";
}

String mqttReadCacheSummary() {
  if (!g_mqtt.cacheDoc.is<JsonObjectConst>()) {
    return "@MQTT=ERR:NO_TOPIC_DATA!";
  }

  JsonObjectConst root = g_mqtt.cacheDoc.as<JsonObjectConst>();
  if (root.size() == 0) {
    return "@MQTT=ERR:NO_TOPIC_DATA!";
  }

  const String json = mqttJsonLiteral(g_mqtt.cacheDoc.as<JsonVariantConst>(), 720);
  return "@MQTT=RX:JSON=" + sanitizeResponseValue(json, 720) + "!";
}

String mqttReadPreferredSummary() {
  String topics[16];
  size_t count = 0;
  if (parseTopicList(g_cfg.mqttTopics, topics, 16, count)) {
    for (size_t i = 0; i < count; ++i) {
      String payload;
      if (mqttReadRawTopicFromCache(topics[i], payload)) {
        return "@MQTT=RX:JSON=" + sanitizeResponseValue(payload, 720) + "!";
      }
    }
  }
  return mqttReadCacheSummary();
}

void sendLine(const String &line, ReplyChannel channel = ReplyChannel::Response);

void saveConfig() {
  File f = LittleFS.open(kCfgPath, "w");
  if (!f) return;
  f.println("[wifi]");
  f.printf("join_enabled = %d\n", g_cfg.wifiJoinEnabled ? 1 : 0);
  f.printf("ssid = \"%s\"\n", tomlEscape(g_cfg.wifiSsid).c_str());
  f.printf("password = \"%s\"\n\n", tomlEscape(g_cfg.wifiPassword).c_str());
  f.println("[mqtt]");
  f.printf("enabled = %d\n", g_cfg.mqttEnabled ? 1 : 0);
  f.printf("name = \"%s\"\n", tomlEscape(g_cfg.mqttName).c_str());
  f.printf("uri = \"%s\"\n", tomlEscape(g_cfg.mqttUriHost).c_str());
  f.printf("port = %u\n", g_cfg.mqttPort);
  f.printf("cache_topics = \"%s\"\n\n", tomlEscape(g_cfg.mqttCacheTopics).c_str());
  f.println("[gps]");
  f.printf("enabled = %d\n", g_cfg.gpsEnabled ? 1 : 0);
  f.printf("baud = %lu\n\n", static_cast<unsigned long>(g_cfg.gpsBaud));
  f.println("[link]");
  f.printf("mode = \"%s\"\n", modeToString(g_cfg.linkMode).c_str());
  f.printf("node_id = %u\n", g_cfg.nodeId);
  f.printf("rs485_baud = %lu\n", static_cast<unsigned long>(g_cfg.rs485Baud));
  f.printf("rs485_dir_gpio = %d\n", g_cfg.rs485DirGpio);
  f.printf("can_bitrate = %lu\n\n", static_cast<unsigned long>(g_cfg.canBitrate));
  f.println("[debug]");
  f.printf("enabled = %d\n", g_cfg.debugEnabled ? 1 : 0);
  f.printf("verbose = %d\n\n", g_cfg.debugVerbose ? 1 : 0);
  f.println("[logging]");
  f.printf("sht4x_addrs = \"%s\"\n", tomlEscape(g_cfg.sht4xAddrs).c_str());
  f.printf("bmp280_addrs = \"%s\"\n", tomlEscape(g_cfg.bmp280Addrs).c_str());
  f.printf("mqtt_topics = \"%s\"\n", tomlEscape(g_cfg.mqttTopics).c_str());
  f.close();
}

void applyKeyValue(const String &section, const String &key, const String &value) {
  if (section == "wifi") {
    if (key == "join_enabled") g_cfg.wifiJoinEnabled = parseTomlBool(value);
    else if (key == "ssid") g_cfg.wifiSsid = value;
    else if (key == "password") g_cfg.wifiPassword = value;
  } else if (section == "mqtt") {
    if (key == "enabled") g_cfg.mqttEnabled = parseTomlBool(value);
    else if (key == "name") g_cfg.mqttName = value;
    else if (key == "uri") g_cfg.mqttUriHost = value;
    else if (key == "port") g_cfg.mqttPort = static_cast<uint16_t>(value.toInt());
    else if (key == "cache_topics") g_cfg.mqttCacheTopics = value;
  } else if (section == "gps") {
    if (key == "enabled") g_cfg.gpsEnabled = parseTomlBool(value);
    else if (key == "baud") g_cfg.gpsBaud = static_cast<uint32_t>(value.toInt());
  } else if (section == "link") {
    if (key == "mode") g_cfg.linkMode = parseMode(value);
    else if (key == "node_id") g_cfg.nodeId = static_cast<uint8_t>(value.toInt());
    else if (key == "rs485_baud") g_cfg.rs485Baud = static_cast<uint32_t>(value.toInt());
    else if (key == "rs485_dir_gpio") g_cfg.rs485DirGpio = value.toInt();
    else if (key == "can_bitrate") g_cfg.canBitrate = static_cast<uint32_t>(value.toInt());
  } else if (section == "debug") {
    if (key == "enabled") g_cfg.debugEnabled = parseTomlBool(value);
    else if (key == "verbose") g_cfg.debugVerbose = parseTomlBool(value);
  } else if (section == "logging") {
    if (key == "sht4x_addrs") g_cfg.sht4xAddrs = value;
    else if (key == "bmp280_addrs") g_cfg.bmp280Addrs = value;
    else if (key == "mqtt_topics") g_cfg.mqttTopics = value;
  }
}

void loadConfig() {
  if (!LittleFS.exists(kCfgPath)) {
    saveConfig();
    return;
  }
  File f = LittleFS.open(kCfgPath, "r");
  if (!f) return;
  String section;
  while (f.available()) {
    String line = trimCopy(f.readStringUntil('\n'));
    if (line.isEmpty() || line.startsWith("#")) continue;
    if (line.startsWith("[") && line.endsWith("]")) {
      section = line.substring(1, line.length() - 1);
      section.trim();
      continue;
    }
    int eq = line.indexOf('=');
    if (eq < 0) continue;
    String key = trimCopy(line.substring(0, eq));
    String value = trimCopy(line.substring(eq + 1));
    if (value.startsWith("\"") && value.endsWith("\"")) value = value.substring(1, value.length() - 1);
    applyKeyValue(section, key, value);
  }
  f.close();
}

bool beginRs485() {
  LinkSerial.end();
  LinkSerial.begin(g_cfg.rs485Baud, SERIAL_8N1, kPortA1Rx, kPortA1Tx);
  g_rs485RxLine = "";
  return true;
}

bool beginCan() {
  twai_general_config_t general = TWAI_GENERAL_CONFIG_DEFAULT(static_cast<gpio_num_t>(kPortA1Tx), static_cast<gpio_num_t>(kPortA1Rx), TWAI_MODE_NORMAL);
  general.intr_flags = ESP_INTR_FLAG_LOWMED;
  general.tx_queue_len = 64;
  general.rx_queue_len = 64;
  twai_timing_config_t timing = TWAI_TIMING_CONFIG_500KBITS();
  if (g_cfg.canBitrate == 250000) timing = TWAI_TIMING_CONFIG_250KBITS();
  else if (g_cfg.canBitrate == 1000000) timing = TWAI_TIMING_CONFIG_1MBITS();
  twai_filter_config_t filter = TWAI_FILTER_CONFIG_ACCEPT_ALL();
  twai_stop();
  twai_driver_uninstall();
  esp_err_t err = twai_driver_install(&general, &timing, &filter);
  if (err != ESP_OK) {
    g_canLastInitErr = err;
    Serial.printf("@CAN=ERR:INIT:%s!\n", esp_err_to_name(err));
    return false;
  }
  err = twai_start();
  if (err != ESP_OK) {
    twai_driver_uninstall();
    g_canLastInitErr = err;
    Serial.printf("@CAN=ERR:START:%s!\n", esp_err_to_name(err));
    return false;
  }
  g_canLastInitErr = ESP_OK;
  g_canLastTxErr = ESP_OK;
  g_canRxFrames = 0;
  g_canRxSeqErrors = 0;
  g_canRxAddrReject = 0;
  Serial.printf("@CAN=OK:READY:TX=%d,RX=%d,BR=%lu!\n", kPortA1Tx, kPortA1Rx, static_cast<unsigned long>(g_cfg.canBitrate));
  return true;
}

bool beginLink() {
  g_linkReady = g_cfg.linkMode == LinkMode::CAN ? beginCan() : beginRs485();
  return g_linkReady;
}

bool canSendText(uint32_t id, const String &text) {
  uint8_t seq = 0;
  size_t offset = 0;
  while (offset < text.length() || (text.isEmpty() && seq == 0)) {
    twai_message_t msg = {};
    msg.identifier = id;
    msg.data[0] = seq;
    msg.data[1] = 0;
    if (offset == 0) msg.data[1] |= jf_coworker::kCanFlagSof;
    size_t chunk = 0;
    while (chunk < 6 && offset < text.length()) {
      msg.data[2 + chunk] = static_cast<uint8_t>(text[offset++]);
      chunk++;
    }
    msg.data_length_code = 2 + chunk;
    if (offset >= text.length()) msg.data[1] |= jf_coworker::kCanFlagEof;
    const esp_err_t txErr = twai_transmit(&msg, pdMS_TO_TICKS(200));
    if (txErr != ESP_OK) {
      g_canLastTxErr = txErr;
      if (g_cfg.debugEnabled) {
        Serial.printf("@DBG=CAN:TX_ERR:%s,ID=0x%03lX,SEQ=%u!\n", esp_err_to_name(txErr), static_cast<unsigned long>(id), static_cast<unsigned>(msg.data[0]));
      }
      return false;
    }
    if (g_cfg.debugEnabled && g_cfg.debugVerbose) {
      Serial.printf("@DBG=CAN:TX_OK,ID=0x%03lX,DLC=%u,SEQ=%u,FLAGS=0x%02X!\n",
                    static_cast<unsigned long>(id),
                    static_cast<unsigned>(msg.data_length_code),
                    static_cast<unsigned>(msg.data[0]),
                    static_cast<unsigned>(msg.data[1]));
    }
    g_canLastTxErr = ESP_OK;
    seq++;
    if (text.isEmpty()) return true;
  }
  return true;
}

void sendLine(const String &line, ReplyChannel channel) {
  String out = line;
  if (channel == ReplyChannel::Response && !g_rs485ReplyPrefix.isEmpty()) {
    out = g_rs485ReplyPrefix + out;
  }

  bool sendToLink = g_commandSource != CommandSource::Local;
  if (channel == ReplyChannel::Debug && g_commandSource == CommandSource::Local) {
    // Keep local diagnostics on USB serial; do not flood the field bus.
    sendToLink = false;
  }
  if (sendToLink) {
    if (g_cfg.linkMode == LinkMode::CAN) {
      const uint32_t id = channel == ReplyChannel::Debug ? jf_coworker::kCanIdCoworkerDebug : jf_coworker::kCanIdCoworkerToHub;
      const bool ok = canSendText(id, out + "\n");
      if (!ok && channel == ReplyChannel::Response) {
        Serial.println("@CAN=ERR:RESP_TX!");
      }
    } else {
      LinkSerial.print(out);
      LinkSerial.print('\n');
    }
  }
  Serial.println(out);
}

void sendDebug(const String &line) {
  if (g_cfg.debugEnabled) sendLine(line, ReplyChannel::Debug);
}

void mqttCallback(char *topic, uint8_t *payload, unsigned int length) {
  const String incomingTopic = topic ? topic : "";
  String incomingPayload;
  incomingPayload.reserve(length);
  for (unsigned int i = 0; i < length; ++i) incomingPayload += static_cast<char>(payload[i]);

  const uint32_t sampleMs = millis();
  String cacheTopics[16];
  String cachePayloads[16];
  size_t cacheCount = 0;
  mqttResolveConfiguredTopicPayloads(g_cfg.mqttCacheTopics,
                                     incomingTopic,
                                     incomingPayload,
                                     cacheTopics,
                                     cachePayloads,
                                     16,
                                     cacheCount);
  for (size_t i = 0; i < cacheCount; ++i) {
    mqttStoreRawSample(cacheTopics[i], cachePayloads[i], sampleMs);
    mqttCacheStoreTopicPayload(cacheTopics[i], cachePayloads[i]);
  }

  String summaryTopics[16];
  String summaryPayloads[16];
  size_t summaryCount = 0;
  mqttResolveConfiguredTopicPayloads(g_cfg.mqttTopics,
                                     incomingTopic,
                                     incomingPayload,
                                     summaryTopics,
                                     summaryPayloads,
                                     16,
                                     summaryCount);
  if (summaryCount == 0) {
    return;
  }

  g_mqtt.lastTopic = summaryTopics[0];
  g_mqtt.lastPayload = summaryPayloads[0];
  for (size_t i = 0; i < summaryCount; ++i) {
    mqttCollectMessageFields(summaryTopics[i], summaryPayloads[i]);
  }
  g_mqtt.lastRxMs = sampleMs;
  g_mqtt.lastRxLine = buildMqttRxLine();
}

void ensureWifiAndMqtt() {
  if (g_cfg.wifiJoinEnabled && WiFi.status() != WL_CONNECTED) {
    static uint32_t lastWifiTry = 0;
    if (millis() - lastWifiTry > 5000) {
      lastWifiTry = millis();
      WiFi.mode(WIFI_STA);
      WiFi.begin(g_cfg.wifiSsid.c_str(), g_cfg.wifiPassword.c_str());
    }
  }
  if (!g_cfg.mqttEnabled || WiFi.status() != WL_CONNECTED) {
    g_mqtt.connected = false;
    return;
  }
  MqttClient.setServer(g_cfg.mqttUriHost.c_str(), g_cfg.mqttPort);
  MqttClient.setCallback(mqttCallback);
  MqttClient.setBufferSize(kMqttClientBufferSize);
  if (!MqttClient.connected()) {
    static uint32_t lastMqttTry = 0;
    if (millis() - lastMqttTry > 3000) {
      lastMqttTry = millis();
      const String clientId = "JUUSOFLUX_COWORKER-" + String(static_cast<uint32_t>(ESP.getEfuseMac()), HEX);
      if (MqttClient.connect(clientId.c_str())) {
        String topics[32];
        size_t count = 0;
        mqttAppendSubscriptionRoots(g_cfg.mqttTopics, topics, 32, count);
        mqttAppendSubscriptionRoots(g_cfg.mqttCacheTopics, topics, 32, count);

        for (size_t i = 0; i < count; ++i) {
          MqttClient.subscribe(topics[i].c_str());
          if (topics[i].indexOf('#') < 0 && topics[i].indexOf('+') < 0) {
            const String descendants = topics[i].endsWith("/") ? topics[i] + "#" : topics[i] + "/#";
            MqttClient.subscribe(descendants.c_str());
          }
        }
        sendDebug("@DBG=MQTT:CONNECTED!");
      }
    }
  }
  g_mqtt.connected = MqttClient.connected();
  if (g_mqtt.connected) MqttClient.loop();
}

void pumpGps() {
  if (!g_cfg.gpsEnabled) return;
  while (GpsSerial.available()) {
    if (Gps.encode(static_cast<char>(GpsSerial.read()))) {
      if (Gps.location.isValid()) {
        g_gps.fix = true;
        g_gps.lat = Gps.location.lat();
        g_gps.lon = Gps.location.lng();
      }
      if (Gps.altitude.isValid()) g_gps.altM = Gps.altitude.meters();
      if (Gps.satellites.isValid()) g_gps.sats = Gps.satellites.value();
      if (Gps.date.isValid() && Gps.time.isValid()) {
        char utcBuf[24];
        snprintf(utcBuf,
                 sizeof(utcBuf),
                 "%04d-%02d-%02dT%02d:%02d:%02dZ",
                 Gps.date.year(),
                 Gps.date.month(),
                 Gps.date.day(),
                 Gps.time.hour(),
                 Gps.time.minute(),
                 Gps.time.second());
        g_gps.utcTime = String(utcBuf);
      }
      g_gps.lastRxMs = millis();
    }
  }
}

void updateI2cSummary() {
  g_i2c = {};
  bool firstDetected = true;
  for (uint8_t addr = 1; addr < 0x78; ++addr) {
    if (i2cProbeAddr(addr)) {
      if (!firstDetected) g_i2c.detectedAddrs += ',';
      if (addr < 0x10) g_i2c.detectedAddrs += '0';
      g_i2c.detectedAddrs += String(addr, HEX);
      firstDetected = false;
    }
  }
  String tokens[8];
  size_t count = 0;
  splitCsv(g_cfg.sht4xAddrs, tokens, 8, count);
  for (size_t i = 0; i < count; ++i) {
    uint8_t addr = static_cast<uint8_t>(strtoul(tokens[i].c_str(), nullptr, 16));
    if (i2cProbeAddr(addr)) {
      g_i2c.sht4xPresent = true;
      break;
    }
  }
  splitCsv(g_cfg.bmp280Addrs, tokens, 8, count);
  for (size_t i = 0; i < count; ++i) {
    uint8_t addr = static_cast<uint8_t>(strtoul(tokens[i].c_str(), nullptr, 16));
    if (i2cProbeAddr(addr)) {
      g_i2c.bmp280Present = true;
      break;
    }
  }
}

void reportStateTransitions() {
  const bool wifiConnected = WiFi.status() == WL_CONNECTED;
  if (wifiConnected != g_prevWifiConnected) {
    g_prevWifiConnected = wifiConnected;
    sendDebug(String("@DBG=WIFI:") + (wifiConnected ? "CONNECTED" : "DISCONNECTED") + "!");
  }

  if (g_mqtt.connected != g_prevMqttConnected) {
    g_prevMqttConnected = g_mqtt.connected;
    sendDebug(String("@DBG=MQTT:") + (g_mqtt.connected ? "CONNECTED" : "DISCONNECTED") + "!");
  }

  if (g_gps.fix != g_prevGpsFix) {
    g_prevGpsFix = g_gps.fix;
    sendDebug(String("@DBG=GPS:") + (g_gps.fix ? "FIX" : "NOFIX") + "!");
  }
}

String getConfigValue(const String &name) {
  if (name == "wifi.join_enabled") return String(g_cfg.wifiJoinEnabled ? 1 : 0);
  if (name == "wifi.ssid") return g_cfg.wifiSsid;
  if (name == "wifi.password") return g_cfg.wifiPassword;
  if (name == "mqtt.enabled") return String(g_cfg.mqttEnabled ? 1 : 0);
  if (name == "mqtt.name") return g_cfg.mqttName;
  if (name == "mqtt.uri") return g_cfg.mqttUriHost;
  if (name == "mqtt.port") return String(g_cfg.mqttPort);
  if (name == "mqtt.cache_topics") return g_cfg.mqttCacheTopics;
  if (name == "gps.enabled") return String(g_cfg.gpsEnabled ? 1 : 0);
  if (name == "gps.baud") return String(g_cfg.gpsBaud);
  if (name == "link.mode") return modeToString(g_cfg.linkMode);
  if (name == "link.node_id") return String(g_cfg.nodeId);
  if (name == "link.rs485_baud") return String(g_cfg.rs485Baud);
  if (name == "link.rs485_dir_gpio") return String(g_cfg.rs485DirGpio);
  if (name == "link.can_bitrate") return String(g_cfg.canBitrate);
  if (name == "debug.enabled") return String(g_cfg.debugEnabled ? 1 : 0);
  if (name == "debug.verbose") return String(g_cfg.debugVerbose ? 1 : 0);
  if (name == "logging.sht4x_addrs") return g_cfg.sht4xAddrs;
  if (name == "logging.bmp280_addrs") return g_cfg.bmp280Addrs;
  if (name == "logging.mqtt_topics") return g_cfg.mqttTopics;
  return "";
}

bool setConfigValue(const String &name, const String &value) {
  if (name == "wifi.join_enabled") g_cfg.wifiJoinEnabled = parseTomlBool(value);
  else if (name == "wifi.ssid") g_cfg.wifiSsid = parseConfigStringValue(value);
  else if (name == "wifi.password") g_cfg.wifiPassword = parseConfigStringValue(value);
  else if (name == "mqtt.enabled") g_cfg.mqttEnabled = parseTomlBool(value);
  else if (name == "mqtt.name") g_cfg.mqttName = parseConfigStringValue(value);
  else if (name == "mqtt.uri") g_cfg.mqttUriHost = parseConfigStringValue(value);
  else if (name == "mqtt.port") g_cfg.mqttPort = static_cast<uint16_t>(value.toInt());
  else if (name == "mqtt.cache_topics") g_cfg.mqttCacheTopics = parseConfigStringValue(value);
  else if (name == "gps.enabled") g_cfg.gpsEnabled = parseTomlBool(value);
  else if (name == "gps.baud") g_cfg.gpsBaud = static_cast<uint32_t>(value.toInt());
  else if (name == "link.mode") g_cfg.linkMode = parseMode(parseConfigStringValue(value));
  else if (name == "link.node_id") g_cfg.nodeId = static_cast<uint8_t>(value.toInt());
  else if (name == "link.rs485_baud") g_cfg.rs485Baud = static_cast<uint32_t>(value.toInt());
  else if (name == "link.rs485_dir_gpio") g_cfg.rs485DirGpio = value.toInt();
  else if (name == "link.can_bitrate") g_cfg.canBitrate = static_cast<uint32_t>(value.toInt());
  else if (name == "debug.enabled") g_cfg.debugEnabled = parseTomlBool(value);
  else if (name == "debug.verbose") g_cfg.debugVerbose = parseTomlBool(value);
  else if (name == "logging.sht4x_addrs") g_cfg.sht4xAddrs = parseConfigStringValue(value);
  else if (name == "logging.bmp280_addrs") g_cfg.bmp280Addrs = parseConfigStringValue(value);
  else if (name == "logging.mqtt_topics") g_cfg.mqttTopics = parseConfigStringValue(value);
  else return false;
  return true;
}

void handleCommand(const String &line) {
  if (line == "@SYS=HELLO!") {
    sendLine("@SYS=OK:JUUSOFLUX_COWORKER!");
  } else if (line == "@CAN=STATUS!") {
    twai_status_info_t st = {};
    if (g_cfg.linkMode == LinkMode::CAN && g_linkReady) {
      twai_get_status_info(&st);
    }
    sendLine("@CAN=STATUS:MODE=" + modeToString(g_cfg.linkMode) +
             ",READY=" + String(g_linkReady ? 1 : 0) +
             ",BR=" + String(g_cfg.canBitrate) +
             ",TXGPIO=" + String(kPortA1Tx) +
             ",RXGPIO=" + String(kPortA1Rx) +
             ",STATE=" + String(static_cast<int>(st.state)) +
             ",QTX=" + String(st.msgs_to_tx) +
             ",QRX=" + String(st.msgs_to_rx) +
             ",TEC=" + String(st.tx_error_counter) +
             ",REC=" + String(st.rx_error_counter) +
             ",TX_FAIL=" + String(st.tx_failed_count) +
             ",BUS_ERR=" + String(st.bus_error_count) +
             ",ARB_LOST=" + String(st.arb_lost_count) +
             ",RX_MISSED=" + String(st.rx_missed_count) +
             ",RX_OVR=" + String(st.rx_overrun_count) +
             ",RX_FRAMES=" + String(g_canRxFrames) +
             ",RX_SEQ_ERR=" + String(g_canRxSeqErrors) +
             ",RX_ADDR_REJ=" + String(g_canRxAddrReject) +
             ",LAST_INIT=" + String(esp_err_to_name(g_canLastInitErr)) +
             ",LAST_TX=" + String(esp_err_to_name(g_canLastTxErr)) +
             "!");
  } else if (line == "@CAN=REINIT!") {
    const bool ok = beginCan();
    sendLine(ok ? "@CAN=OK:REINIT!" : ("@CAN=ERR:REINIT:" + String(esp_err_to_name(g_canLastInitErr)) + "!"));
  } else if (line == "@CAN=TXTEST!") {
    if (g_cfg.linkMode != LinkMode::CAN || !g_linkReady) {
      sendLine("@CAN=ERR:NOT_READY!");
      return;
    }
    twai_message_t msg = {};
    msg.identifier = 0x321;
    msg.data_length_code = 2;
    msg.data[0] = 0x5A;
    msg.data[1] = 0xA5;
    const esp_err_t err = twai_transmit(&msg, pdMS_TO_TICKS(40));
    if (err == ESP_OK) {
      g_canLastTxErr = ESP_OK;
      sendLine("@CAN=OK:TXTEST!");
    } else {
      g_canLastTxErr = err;
      sendLine("@CAN=ERR:TXTEST:" + String(esp_err_to_name(err)) + "!");
    }
  } else if (line == "@LINK=STATUS!") {
    sendLine("@LINK=STATUS:MODE=" + modeToString(g_cfg.linkMode) + ",READY=" + String(g_linkReady ? 1 : 0) + ",NODE_ID=" + String(g_cfg.nodeId) + "!");
  } else if (line == "@LINK=ADDR!") {
    sendLine("@LINK=ADDR:" + String(g_cfg.nodeId) + "!");
  } else if (line == "@WIFI=STATUS!") {
    sendLine("@WIFI=STATUS:JOIN=" + String(g_cfg.wifiJoinEnabled ? 1 : 0) + ",CONNECTED=" + String(WiFi.status() == WL_CONNECTED ? 1 : 0) + ",SSID=" + g_cfg.wifiSsid + "!");
  } else if (line == "@MQTT=STATUS!") {
    sendLine("@MQTT=STATUS:EN=" + String(g_cfg.mqttEnabled ? 1 : 0) + ",CONNECTED=" + String(g_mqtt.connected ? 1 : 0) + ",TOPIC=" + (g_mqtt.lastTopic.isEmpty() ? String("-") : g_mqtt.lastTopic) + "!");
  } else if (line == "@MQTT=LAST!") {
    sendLine("@MQTT=LAST:TOPIC=" + (g_mqtt.lastTopic.isEmpty() ? String("-") : g_mqtt.lastTopic) + ",AGE_MS=" + formatAgeMs(g_mqtt.lastRxMs) + ",PAYLOAD=" + (g_mqtt.lastPayload.isEmpty() ? String("-") : g_mqtt.lastPayload) + "!");
  } else if (line == "@MQTT=READ!") {
    sendLine(mqttReadPreferredSummary());
  } else if (line.startsWith("@MQTT=READ:") && line.endsWith("!")) {
    String topic = line.substring(strlen("@MQTT=READ:"), line.length() - 1);
    topic.trim();
    if (topic.isEmpty()) {
      sendLine("@MQTT=ERR:BAD_TOPIC!");
    } else {
      sendLine(mqttReadRawTopic(topic));
    }
  } else if (line == "@GPS=STATUS!") {
    sendLine("@GPS=STATUS:EN=" + String(g_cfg.gpsEnabled ? 1 : 0) + ",FIX=" + String(g_gps.fix ? 1 : 0) + ",SATS=" + String(g_gps.sats) + "!");
  } else if (line == "@GPS=RAW!") {
    sendLine("@GPS=RAW:FIX=" + String(g_gps.fix ? 1 : 0) + ",SATS=" + String(g_gps.sats) + ",LAT=" + String(g_gps.lat, 6) + ",LON=" + String(g_gps.lon, 6) + ",ALT_M=" + String(g_gps.altM, 1) + ",AGE_MS=" + formatAgeMs(g_gps.lastRxMs) + "!");
  } else if (line == "@I2C=SCAN!") {
    updateI2cSummary();
    sendLine("@I2C=STATUS:SHT4X=" + String(g_i2c.sht4xPresent ? 1 : 0) + ",BMP280=" + String(g_i2c.bmp280Present ? 1 : 0) + "!");
  } else if (line == "@I2C=LIST!") {
    updateI2cSummary();
    sendLine("@I2C=LIST:" + (g_i2c.detectedAddrs.isEmpty() ? String("-") : g_i2c.detectedAddrs) + "!");
  } else if (line == "@I2C=SHT4X:READ!") {
    refreshSensorCaches(true);
    if (!g_sht4x.valid || !isFresh(g_sht4x.sampleMs)) {
      sendLine("@I2C=SHT4X:ERR:NO_DEVICE!");
      return;
    }
    sendLine("@I2C=SHT4X#" + hexAddr(g_sht4x.addr) + ":TEMP_C=" + String(g_sht4x.tempC, 2) + ",RH_PCT=" + String(g_sht4x.rhPct, 2) + ",AGE_MS=" + formatAgeMs(g_sht4x.sampleMs) + "!");
  } else if (line == "@I2C=BMP280:READ!") {
    refreshSensorCaches(true);
    if (!g_bmp280.valid || !isFresh(g_bmp280.sampleMs)) {
      sendLine("@I2C=BMP280:ERR:NO_DEVICE!");
      return;
    }
    sendLine("@I2C=BMP280#" + hexAddr(g_bmp280.addr) + ":TEMP_C=" + String(g_bmp280.tempC, 2) + ",PRESS_PA=" + String(g_bmp280.pressurePa, 1) + ",AGE_MS=" + formatAgeMs(g_bmp280.sampleMs) + "!");
  } else if (line == "@POLL=ALL!") {
    updateI2cSummary();
    sendLine("@POLL=ALL:" + buildPollAllFields() + "!");
  } else if (line == "@CFG=SAVE!") {
    saveConfig();
    sendLine("@CFG=OK:SAVED!");
  } else if (line == "@CFG=RELOAD!") {
    loadConfig();
    beginLink();
    sendLine("@CFG=OK:RELOADED!");
  } else if (line == "@DBG=ENABLE!") {
    g_cfg.debugEnabled = true;
    sendLine("@DBG=OK:ENABLED!");
  } else if (line == "@DBG=DISABLE!") {
    g_cfg.debugEnabled = false;
    sendLine("@DBG=OK:DISABLED!");
  } else if (line.startsWith("@CFG=GET:")) {
    const String key = line.substring(9, line.length() - 1);
    const String value = getConfigValue(key);
    sendLine(value.isEmpty() ? "@CFG=ERR:UNKNOWN!" : "@CFG=" + key + "=" + value + "!");
  } else if (line.startsWith("@CFG=SET:")) {
    const String body = line.substring(9, line.length() - 1);
    const int eq = body.indexOf('=');
    if (eq < 0) {
      sendLine("@CFG=ERR:FORMAT!");
      return;
    }
    const String key = body.substring(0, eq);
    const String value = body.substring(eq + 1);
    if (!setConfigValue(key, value)) {
      sendLine("@CFG=ERR:UNKNOWN!");
      return;
    }
    if (key.startsWith("link.")) beginLink();
    sendLine("@CFG=OK:" + key + "=" + value + "!");
  } else {
    sendLine("@ERR=UNKNOWN!");
  }
}

void pumpRs485Commands() {
  while (LinkSerial.available()) {
    char ch = static_cast<char>(LinkSerial.read());
    if (ch == '\r') continue;
    if (ch == '\n') {
      const String line = trimCopy(g_rs485RxLine);
      g_rs485RxLine = "";
      if (!line.isEmpty()) {
        String command;
        String replyPrefix;
        if (parseRs485Envelope(line, command, replyPrefix)) {
          g_rs485ReplyPrefix = replyPrefix;
          g_commandSource = CommandSource::Rs485;
          handleCommand(command);
          g_commandSource = CommandSource::Local;
          g_rs485ReplyPrefix = "";
        }
      }
    } else if (g_rs485RxLine.length() < 256) {
      g_rs485RxLine += ch;
    }
  }
}

void pumpUsbCommands() {
  while (Serial.available()) {
    char ch = static_cast<char>(Serial.read());
    if (ch == '\r') continue;
    if (ch == '\n') {
      const String line = trimCopy(g_usbRxLine);
      g_usbRxLine = "";
      if (!line.isEmpty()) {
        g_commandSource = CommandSource::Local;
        handleCommand(line);
      }
    } else if (g_usbRxLine.length() < 256) {
      g_usbRxLine += ch;
    }
  }
}

void pumpCanCommands() {
  twai_message_t msg;
  while (twai_receive(&msg, 0) == ESP_OK) {
    g_canRxFrames++;
    if (g_cfg.debugEnabled && g_cfg.debugVerbose) {
      Serial.printf("@DBG=CAN:RX,ID=0x%03lX,DLC=%u,SEQ=%u,FLAGS=0x%02X!\n",
                    static_cast<unsigned long>(msg.identifier),
                    static_cast<unsigned>(msg.data_length_code),
                    static_cast<unsigned>(msg.data[0]),
                    static_cast<unsigned>(msg.data[1]));
    }
    if (msg.identifier != jf_coworker::kCanIdHubToCoworker || msg.data_length_code < 2) continue;
    const uint8_t seq = msg.data[0];
    const uint8_t flags = msg.data[1];
    if (flags & jf_coworker::kCanFlagSof) {
      g_canRxLine = "";
      g_canRxSeq = seq;
    }
    if (seq != g_canRxSeq) {
      g_canRxSeqErrors++;
      g_canRxLine = "";
      continue;
    }
    for (int i = 2; i < msg.data_length_code; ++i) {
      if (msg.data[i] != '\0') g_canRxLine += static_cast<char>(msg.data[i]);
    }
    g_canRxSeq++;
    if (flags & jf_coworker::kCanFlagEof) {
      const String line = trimCopy(g_canRxLine);
      g_canRxLine = "";
      if (!line.isEmpty()) {
        String command;
        String replyPrefix;
        if (parseRs485Envelope(line, command, replyPrefix)) {
          g_rs485ReplyPrefix = replyPrefix;
          g_commandSource = CommandSource::Can;
          handleCommand(command);
          g_commandSource = CommandSource::Local;
          g_rs485ReplyPrefix = "";
        } else {
          g_canRxAddrReject++;
          if (g_cfg.debugEnabled && g_cfg.debugVerbose) {
            sendDebug("@DBG=CAN:ENVELOPE_REJECT:" + line + "!");
          }
        }
      }
    }
  }
}

}  // namespace

void setup() {
  auto cfg = M5.config();
  M5.begin(cfg);
  Serial.begin(115200);
  delay(200);
  Wire.begin(kPortA2Sda, kPortA2Scl, 100000);
  if (!LittleFS.begin(true)) {
    Serial.println("@SYS=ERR:LITTLEFS!");
  }
  loadConfig();
  GpsSerial.begin(g_cfg.gpsBaud, SERIAL_8N1, kGpsRx, kGpsTx);
  beginLink();
  refreshSensorCaches(true);
  updateI2cSummary();
  sendLine("@BOOT=OK:JUUSOFLUX_COWORKER!");
  sendLine("@I2C=STATUS:SHT4X=" + String(g_i2c.sht4xPresent ? 1 : 0) + ",BMP280=" + String(g_i2c.bmp280Present ? 1 : 0) + "!");
}

void loop() {
  pumpUsbCommands();

  if (g_cfg.linkMode == LinkMode::CAN) pumpCanCommands();
  else pumpRs485Commands();

  pumpGps();
  ensureWifiAndMqtt();
  refreshSensorCaches();
  updateI2cSummary();
  reportStateTransitions();

  static uint32_t lastAlive = 0;
  if (g_cfg.debugEnabled && millis() - lastAlive > 5000) {
    lastAlive = millis();
    sendDebug("@DBG=ALIVE!");
  }
  delay(10);
}