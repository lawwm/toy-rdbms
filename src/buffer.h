#pragma once

#include <string>
#include <vector>
#include <iostream>
#include <fstream>
#include <cstring>
#include <stdexcept>
#include <unordered_map>
#include <filesystem>

#include "common.h"
#include "query.h"

// this page stores the storage engine.

// strategy -> store everything on one file and use it as a disk...

// just store some stuff...

// metadata page

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


class FileManager {
private:
  u32 blockSize;
  std::unordered_map<std::string, std::fstream> fileMap;

  std::fstream& seekFile(PageId pageId);

public:
  FileManager(u32 blockSize) : blockSize{ blockSize } {}
  ~FileManager() {
    for (auto& file : fileMap) {
      file.second.close();
    }
  }

  FileManager(const FileManager& other) = delete;
  FileManager& operator=(const FileManager& other) = delete;
  FileManager(FileManager&& other) = delete;
  FileManager& operator=(FileManager&& other) = delete;

  u32 getBlockSize() {
    return blockSize;
  }

  bool read(PageId pageId, std::vector<char>& bufferData);

  bool write(PageId pageId, std::vector<char>& bufferData);

  // return the last pageId
  u32 append(std::string filename, int numberOfBlocksToAppend = 1);

  u32 getNumberOfPages(PageId pageId);
  u32 getNumberOfPages(std::string filename);

  void createFileIfNotExists(const std::string& fileName);

  bool doesFileExists(const std::string& fileName);

  bool deleteFile(const std::string& fileName);
};


struct BufferFrame {
  size_t bufferSize;
  PageId pageId;
  std::vector<char> bufferData;
  int pin;
  bool dirty;

  BufferFrame(size_t bufferSize) : bufferSize{ bufferSize }, pageId{ emptyPageId }, pin{ 0 }, dirty{ false } {
    bufferData.resize(bufferSize);
  }

  ~BufferFrame() {}

  void modify(const void* data, u64 length, u64 offset) {
    std::memcpy(bufferData.data() + offset, data, length);
    dirty = true;
  }
};


class BufferManager {
private:
  u32 bufferSize;
  std::vector<BufferFrame> bufferPool;

public:
  // todo LRU, now just find first.
  BufferManager(u32 bufferSize, u32 poolSize) : bufferSize{ bufferSize }, bufferPool(poolSize, BufferFrame(bufferSize)) {

  }

  // if pageId doesn't exist, return nullptr.
  BufferFrame* pin(FileManager& fileManager, PageId pageId) {
    // if already pinned, return the buffer
    // if not pinned, find a new buffer.
    // if all buffers are pinned, return nullptr.
    int existingBufferIndex = -1, unpinnedBufferIndex = -1;
    for (int i = 0; i < bufferPool.size(); ++i) {
      auto& buffer = bufferPool[i];
      if (buffer.pin == 0) {
        unpinnedBufferIndex = i;
      }
      if (buffer.pageId == pageId) {
        existingBufferIndex = i;
      }
    }

    if (existingBufferIndex != -1) {
      bufferPool[existingBufferIndex].pin++;
      return &bufferPool[existingBufferIndex];
    }

    if (unpinnedBufferIndex != -1) {
      if (!fileManager.read(pageId, bufferPool[unpinnedBufferIndex].bufferData)) {
        return nullptr;
      }
      bufferPool[unpinnedBufferIndex].pageId = pageId;
      bufferPool[unpinnedBufferIndex].pin++;
      return &bufferPool[unpinnedBufferIndex];
    }

    // wait here...
    return nullptr;
  }

  bool unpin(FileManager& fileManager, PageId pageId) {
    for (auto& buffer : bufferPool) {
      if (buffer.pageId == pageId) {
        buffer.pin--;
        if (buffer.pin == 0 && buffer.dirty) {
          // write to disk
          buffer.dirty = false;
          // page number doesn't exist
          if (!fileManager.write(pageId, buffer.bufferData)) {
            return false;
          }
        }
        return true;
      }
    }

    // buffer doesn't exist
    return false;
  }
};

struct ResourceManager {
  FileManager fm;
  BufferManager bm;

  ResourceManager(u32 pagesize, u32 poolsize) : fm{ pagesize }, bm{ pagesize, poolsize } {}
};
