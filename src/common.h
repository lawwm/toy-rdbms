#pragma once

#include <cstdint>
#include <limits>
#include <string>
#include <vector>
#include <iostream>
#include <fstream>
#include <cstring>
#include <stdexcept>
#include <unordered_map>
#include <filesystem>

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

struct PageId {
  std::string filename;
  u64 pageNumber;
  bool operator==(const PageId& other) const {
    return filename == other.filename && pageNumber == other.pageNumber;
  }
};

const static PageId emptyPageId = { "", u64Max };

const static size_t TEST_PAGE_SIZE = 512;
const static size_t PAGE_SIZE_S = 4096;
const static size_t PAGE_SIZE_M = 8192;
const static size_t PAGE_SIZE_L = 16384;

const static size_t FIRST_BIT = 0x80000000;
const static size_t ALL_OTHER_BITS = 0x7FFFFFFF;

// one header page contains 4mb
struct RID {
  u64 pageNumber;
  u32 slotNumber;
};

enum class PageType {
  TuplePage, IndexPage, DirectoryPage
};

struct PageDirectory {
  PageType pageType;
  u64 nextPage;
  u64 prevPage;
  u64 numberOfEntries;
  u64 pageDirNumber;
  char tableName[128];

  PageDirectory(u64 nextPage, u64 prevPage, u64 numberOfEntries, u64 pageDirNum, std::string filename) :
    pageType{ PageType::DirectoryPage },
    nextPage{ nextPage }, prevPage{ prevPage }, numberOfEntries{ numberOfEntries }, pageDirNumber{ pageDirNum } {
    std::strncpy(this->tableName, filename.c_str(), 128);
  }
};

struct PageEntry {
  u64 pageNumber;
  u32 freeSpace;
};

struct TuplePage {
  PageType pageType;
  u64 checkSum;
  u32 pageSize;
  u32 numberOfSlots;
  u32 lastOccupiedPosition;

  TuplePage(u64 checkSum, u32 pageSize, u32 numberOfSlots, u32 lastOccupiedPosition) :
    pageType{ PageType::TuplePage },
    checkSum{ checkSum }, pageSize{ pageSize }, numberOfSlots{ numberOfSlots }, lastOccupiedPosition{ lastOccupiedPosition } {}
};

struct Slot {
  u32 data;
  bool isOccupied() const {
    return (data & FIRST_BIT) != 0;
  }
  void setOccupied(bool occupied) {
    if (occupied) {
      data |= FIRST_BIT;
    }
    else {
      data &= ALL_OTHER_BITS;
    }
  }
  u32 getOffset() const {
    return data & ALL_OTHER_BITS;
  }
  void setOffset(u32 offset) {
    if (offset >= FIRST_BIT) {
      throw std::runtime_error("Offset is too large");
    }
    data = (data & FIRST_BIT) | (offset & ALL_OTHER_BITS);
  }
};

