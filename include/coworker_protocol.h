#pragma once

#include <Arduino.h>

namespace jf_coworker {

static constexpr uint32_t kCanIdHubToCoworker = 0x501;
static constexpr uint32_t kCanIdCoworkerToHub = 0x502;
static constexpr uint32_t kCanIdCoworkerDebug = 0x503;

static constexpr uint8_t kCanFlagSof = 0x01;
static constexpr uint8_t kCanFlagEof = 0x02;

enum class LinkMode {
  RS485,
  CAN,
};

enum class ReplyChannel {
  Response,
  Debug,
};

}  // namespace jf_coworker
