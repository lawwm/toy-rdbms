#pragma once

#include <cstdint>
#include <limits>

using u8 = std::uint8_t;
using u16 = std::uint16_t;
using u32 = std::uint32_t;
using u64 = std::uint64_t;
using i8 = std::int8_t;
using i16 = std::int16_t;
using i32 = std::int32_t;


const static u8 u8Max = std::numeric_limits<u8>::max();
const static u32 u32Max = std::numeric_limits<u32>::max();
const static u64 u64Max = std::numeric_limits<u64>::max();
