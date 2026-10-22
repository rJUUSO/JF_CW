JUUSOFLUX_COWORKER

PlatformIO firmware for an M5Stack AtomS3 Lite based coworker node that:

- joins a remote analyzer AP and reads MQTT data
- reads local GPS and I2C sensors
- exposes one remote command surface to JUUSOFLUX_HUB, or to ESP_CYBERDECK (an M5Stack Tab5) as a
  JuusoFlux "Coworker" source on one of its serial ports
- uses either RS485 or CAN as the wired uplink to the hub (to the Tab5: RS485 levels on a TTL port)

Purpose

- offload analyzer-specific Wi-Fi and MQTT handling from JUUSOFLUX_HUB
- keep a wired, serviceable path between coworker and hub
- provide a uniform command/config surface to the host through HUB forwarding

Current status

- PlatformIO Arduino project for AtomS3 Lite is operational
- date-based firmware version (`vYYYY.MM.DD`, `@VER!`), releases built by GitHub Actions from a tag
- coworker TOML config is used from `data/coworker.toml` (see "Settings files")
- RS485 and CAN link backends are implemented
- ASCII command interface is implemented for config, debug, status, read, and poll-all
- CAN segmented text protocol is implemented
- CAN diagnostics commands are implemented (`@CAN=STATUS!`, `@CAN=REINIT!`, `@CAN=TXTEST!`)
- GPS UART parsing and snapshot caching are implemented
- MQTT raw/cache ingestion and JSON read views are implemented
- SHT4X and BMP280 reads are implemented with background cache refresh
- remote `@CFG=GET/SET/SAVE/RELOAD` is implemented
- a fixed Wi-Fi address (`[wifi] ip / mask / gateway / dns`, empty `ip` = DHCP)
- time from the host (`@TIME=SET`) and an NTP server with it (`[ntp] enabled`), so the analyzer can
  take its time from the coworker (`@NTP=LICOR!`); see protocol.md "Time and NTP server"
- `@POLL=ALL!` carries `WIFI_rssi` (the signal to the analyzer's AP, dBm) and the analyzer's
  battery: `<name>_battery` (%) and `<name>_ac` (1 on mains), from its retained `system/Battery`
  and `system/AC` MQTT topics, subscribed by themselves
- status LED (the AtomS3 Lite's RGB LED): pulses red while Wi-Fi is down, orange while MQTT is
  down, yellow when ready; flashes blue when a link command comes in and green when a reply goes
  out (RS485 or CAN)

Not yet completed

- CAN transport validation through HUB in field conditions
- HUB-side forwarder integration for coworker debug stream
- link-layer ACK/NACK/retry rules and config CRC/versioning
- watchdog-safe reconnect handling

Recommended topology

- preferred production link: CAN on AtomS3 Port.A1 via CAN transceiver adapter
- fallback / first bring-up link: RS485 on AtomS3 Port.A1 via auto-direction RS485 adapter
- keep HUB Port.C for recovery and maintenance, not as the permanent coworker uplink

Pin plan

- AtomS3 Port.A1:
    - `G2/G1`
    - RS485 link UART or CAN TX/RX to transceiver adapter
- AtomS3 Port.A2:
    - `SDA=G38/SCL=G39`
    - local I2C sensor bus for SHT4X and BMP280
- AtomS3 Port.C:
    - `TX=G5`, `RX=G6`
    - GPS/BDS UART (default `[gps] rx_gpio = 6`, `tx_gpio = 5`)
- Atomic GPS Base under the AtomS3 (AT6558, 9600 baud): the GPS data arrives on `G5`
    - `[gps] baud = 9600`, `rx_gpio = 5`, `tx_gpio = 6`
    - or live: `@CFG=SET:gps.rx_gpio=5!`, `@CFG=SET:gps.tx_gpio=6!`, `@CFG=SET:gps.baud=9600!`, `@CFG=SAVE!`
    - `@GPS=RAW!` `AGE_MS` is the age of the last NMEA sentence, `-1` = none yet (wrong pin or baud);
      the module sends sentences without a fix too

Build

- `pio run`
- `pio run -t upload --upload-port COM11`
- `pio run -t uploadfs --upload-port COM11`
- select an alternate TOML for `uploadfs` by setting `COWORKER_FS_CONFIG`, for example `set COWORKER_FS_CONFIG=coworker.li7820.toml && pio run -e juusoflux-coworker -t uploadfs --upload-port COM11`
- the libraries are pinned (M5Unified 0.2.13, M5GFX 0.2.34, M5AtomS3, FastLED), so a release
  build is the same as a local one

Settings files (`data/`)

- `coworker.toml`: the default for `uploadfs` and the full release image. Since 2026-10-22 it is
  CW20's running settings (read with `@CFG=GET`): the LI-7820 (`TG20-01658`, MQTT name
  `LI7820`) with the Tab5, `link.mode = rs485` at 115200 as node 20 on the Tab5's `ttl_b`, NTP
  server on, DHCP, GPS off, no I2C sensors (they moved to the JF HUB).
- `coworker.li7820.toml`: the same, kept under the analyzer's name.
- `coworker.li7810.toml`: the LI-7810 coworker (`TG10-01250`) on a JF HUB's CAN bus as node 10.
- Keys a file leaves out keep their defaults; `@CFG=SAVE!` writes every key.

Release

- `scripts/create_release_tag.ps1` tags the commit `vYYYY.MM.DD` (UTC) and pushes the tag; one
  release a day (`-UpdateExisting` moves today's tag). `.github/workflows/release.yml` then builds
  the firmware with that version (`JF_FW_VER`, `scripts/set_fw_ver.py`) and publishes:
  - `JF_COWORKER_firmware_<ver>.bin`: the app (flash at 0x10000, or `pio run -t upload`)
  - `JF_COWORKER_<file>_littlefs_<ver>.bin`: a LittleFS image (at 0x670000) per settings file
  - `JF_COWORKER_full_<ver>.bin`: bootloader, partitions, app and `coworker.toml` at 0x0
  - `SHA256SUMS_<ver>.txt`

Commit and push all changes

From `JUUSOFLUX_COWORKER`:

1. Check status:
    - `git status`
2. Stage everything:
    - `git add -A`
3. Commit:
    - `git commit -m "<your message>"`
4. Sync with remote main before push:
    - `git pull --rebase origin main`
5. Push:
    - `git push origin main`


Key runtime commands

- `@SYS=HELLO!`
- `@VER!` (`@VER=COWORKER:v2026.10.22!`, also sent at boot)
- `@CAN=STATUS!`
- `@CAN=REINIT!`
- `@CAN=TXTEST!`
- `@LINK=STATUS!`
- `@LINK=ADDR!`
- `@WIFI=STATUS!`
- `@MQTT=STATUS!`
- `@MQTT=LAST!`
- `@MQTT=READ!`
- `@MQTT=READ:licor/niobrara/output/concentration/Concentration!`
- `@GPS=STATUS!`
- `@GPS=RAW!`
- `@I2C=SCAN!`
- `@I2C=LIST!`
- `@I2C=SHT4X:READ!`
- `@I2C=BMP280:READ!`
- `@POLL=ALL!`
- `@CFG=GET:link.mode!`
- `@CFG=SET:link.mode=can!`
- `@CFG=SET:logging.mqtt_topics=licor/niobrara/output/concentration/Concentration!`
- `@CFG=SAVE!`
- `@CFG=RELOAD!`
- `@DBG=ENABLE!`
- `@DBG=DISABLE!`

Configuration model

The coworker uses a HUB-like TOML config model for the sensors that are actually attached to the AtomS3:

- `sht4x_addrs = "44"`
- `bmp280_addrs = "76"`
- `cache_topics = "licor/niobrara/output/concentration/Concentration"`
- `mqtt_topics = "licor/niobrara/output/concentration/Concentration"`

MQTT subscription behavior

- each configured MQTT topic is subscribed directly
- each cache topic is also subscribed directly
- if a configured topic points at a nested JSON object such as `licor/niobrara/output/concentration/Concentration`, the coworker also subscribes to the immediate parent topic so it can extract that child object from the parent JSON payload
- topic list expansion matches HUB-style bracket syntax, for example `licor/niobrara/output/concentration/[n2o,h2o]`
- `@MQTT=READ:<topic>!` is restricted by `mqtt.cache_topics`
- MQTT cache stores structured JSON under topic path segments
- MQTT fields are flattened dynamically from configured logging topics, so `@POLL=ALL!` exposes discovered fields as `LI7810_<field>=...`
- keyed MQTT arrays are collapsed to their primary value when flattened, so payloads like `"H2O":[40.1,1]` emit `LI7810_h2o=40.1` instead of `LI7810_h2o_0` and `LI7810_h2o_1`

Current read commands

- `@I2C=SHT4X:READ!` returns the latest cached temperature and humidity from the first configured SHT4X address that responds
- `@I2C=BMP280:READ!` returns the latest cached temperature and pressure from the first configured BMP280 address that responds
- `@MQTT=READ!` returns cached JSON for the configured logging topic as `@MQTT=RX:JSON={...}!`
- `@MQTT=READ:<topic>!` returns cached topic JSON as `@MQTT=RX:TOPIC=...,NAME=LI7810,JSON={...}!`
- `@MQTT=READ:<topic>!` returns `@MQTT=ERR:NO_TOPIC_DATA!` when the topic is outside `mqtt.cache_topics` or no cache entry exists
- `@LINK=ADDR!` returns the coworker `link.node_id`
- `@I2C=LIST!` returns currently detected I2C addresses
- `@MQTT=LAST!` returns most recent topic/payload with age
- addressed RS485 requests only reply when the target address matches `link.node_id`, and the response is wrapped as `@<addr:...`

Cache behavior

- MQTT samples are cached on arrival (allow-listed by `mqtt.cache_topics`) and `@POLL=ALL!` replies from that cache rather than waiting for a fresh publish
- SHT4X and BMP280 samples are refreshed in the background and `@POLL=ALL!` replies from the latest fresh cache entry
- stale cache entries are not emitted as current data in `@POLL=ALL!`
- `@POLL=ALL!` only emits GPS fields when `[gps].enabled = 1`
- `@POLL=ALL!` only emits `SHT4X_<addr>_*` and `BMP280_<addr>_*` fields for addresses listed in `[logging]`

Main config groups

- `[wifi]` (`join_enabled`, `ssid`, `password`, `ip`, `mask`, `gateway`, `dns`)
- `[ntp]` (`enabled`: the NTP server for the analyzer)
- `[mqtt]`
- `[gps]` (`enabled`, `baud`, `rx_gpio`, `tx_gpio`; a `gps.*` change restarts the GPS UART)
- `[link]`
- `[debug]`
- `[logging]`

Data-link protocol

Protocol summary is documented in `protocol.md`.

Short version:

- application layer stays ASCII command/response based
- RS485 transport uses newline-delimited ASCII commands
- CAN transport uses segmented ASCII payload frames over TWAI
- HUB can forward host commands to coworker and forward coworker responses/debug back to host without inventing a second config protocol

Roadmap

Detailed roadmap is in `ROADMAP.md`.

Suggested implementation order

1. Validate CAN transport end-to-end through HUB forwarding.
2. Add coworker heartbeat and stale-link timeout handling.
3. Add link-layer ACK/NACK/retry and sequence-gap behavior for noisy buses.
4. Add watchdog-safe reconnect behavior and config revision/CRC reporting.
5. Expand HUB-side merged logging and debug forwarding coverage.