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

// records
// unspanned
// homogenous blocks
// fixed length
// i feel like i don't need offset... for cpp but idk

// keep each table in a different file.



// let's say I want to select * from tableX;
/**
* Creating a table
*
* I create a new heap file.
* I allocate some tuple pages.
*
*
* Inserting a tuple.
*
* I find the table heap file.
* I find the first free page.
* I insert into the tuple page and return my slot
*
*
* Read a table
*
* I find the table heap file.
* I find all pages.
* I read all pages.
*
* Deleting a tuple, delete where x == 2.
*
*
*
*/
// pack page entries


// rn I'm mixing heap file operations with record operations. I should separate the two

// heap page directory -> I have a linkedlist of heap file directory
// heap page -> map
namespace HeapFile {
  // 2x 



  /**
  There are Page Directories, which contains Page Entries.

  Page Entry contains [page number, free space]

  Page Entry leads to Pages.

  Here's the invariant:
  - Do not create another Page Directory until the current Page Directory is full of page entries.

  - pageDirBuffer should always exist. pageBuffer might not exist at any moment in time.

  - when should you create a new page entry or a new page directory?
  - when you don't have enough space.

  Problem with this iterator is that I open two buffers at any point in time.

  */
  class HeapFileIterator {
  private:
    // page directory buffer
    PageId pageDirectoryId;
    BufferFrame* pageDirBuffer;

    // page buffer
    PageId pageBufferId;
    BufferFrame* pageBuffer;
    u32 pageEntryIndex;

    // misc data
    std::string filename;
    std::shared_ptr<ResourceManager> resourceManager;
  public:
    HeapFileIterator(std::string filename, std::shared_ptr<ResourceManager> rm) : pageBuffer{ nullptr }, pageEntryIndex{ u32Max }, filename{ filename },
      resourceManager{ rm } {
      pageDirectoryId = PageId{ filename, 0 };
      pageDirBuffer = resourceManager->bm.pin(resourceManager->fm, pageDirectoryId);
    };

    ~HeapFileIterator() {
      if (resourceManager) {
        resourceManager->bm.unpin(resourceManager->fm, pageDirectoryId);
        if (pageBuffer) {
          resourceManager->bm.unpin(resourceManager->fm, pageBuffer->pageId);
        }
      }
    }

    HeapFileIterator(const HeapFileIterator& other) = delete;
    HeapFileIterator& operator=(const HeapFileIterator& other) = delete;
    HeapFileIterator(HeapFileIterator&& other) = delete;
    HeapFileIterator& operator=(HeapFileIterator&& other) = delete;

    BufferFrame* getPageDirBuffer() {
      return pageDirBuffer;
    };

    u32 getPageEntryIndex() {
      return pageEntryIndex;
    };

    BufferFrame* getPageBuffer() {
      return pageBuffer;
    };

    /**
    * Return true if pages were changed, false if no pages were changed
    *
    */
    bool findFirstDir() {

      // set page buffer to nullptr
      if (pageBuffer) {
        resourceManager->bm.unpin(resourceManager->fm, pageBuffer->pageId);
      }

      if (pageDirectoryId.pageNumber == 0) {
        return true;
      }
      else {
        pageDirectoryId = PageId{ filename, 0 };
        resourceManager->bm.unpin(resourceManager->fm, pageDirBuffer->pageId);
        pageDirBuffer = resourceManager->bm.pin(resourceManager->fm, pageDirectoryId);

        return true;
      }
    };

    void extendHeapFile() {
      // unpin the current page buffer
      if (pageBuffer) {
        resourceManager->bm.unpin(resourceManager->fm, pageBuffer->pageId);
      }

      PageDirectory* pd = (PageDirectory*)pageDirBuffer->bufferData.data();
      while (pd->nextPage != u64Max) {// Find the last page directory
        u64 nextPageIdPageNum = pd->nextPage;
        resourceManager->bm.unpin(resourceManager->fm, this->pageDirectoryId);

        this->pageDirectoryId = PageId{ filename, nextPageIdPageNum };
        this->pageDirBuffer = resourceManager->bm.pin(resourceManager->fm, this->pageDirectoryId);
        pd = (PageDirectory*)this->pageDirBuffer->bufferData.data();
      }

      // create a new page directory
      // fill it fully with pageentry list
      // create a new page
      u64 nextLastPageDir = pd->pageDirNumber * 2;
      u64 currPageDir = pd->pageDirNumber + 1;
      u64 numOfEntries = (resourceManager->fm.getBlockSize() - sizeof(PageDirectory)) / sizeof(PageEntry);
      while (currPageDir <= nextLastPageDir) {
        // set up current page directory
        PageDirectory newPd{ u64Max, this->pageDirectoryId.pageNumber, numOfEntries, currPageDir, filename };
        u32 lastPageNumber = resourceManager->fm.append(filename, 1) - 1;
        pd->nextPage = lastPageNumber;
        this->pageDirBuffer->dirty = true;
        this->resourceManager->bm.unpin(this->resourceManager->fm, this->pageDirBuffer->pageId);

        // set up page entries
        this->pageDirectoryId = PageId{ filename, lastPageNumber };
        this->pageDirBuffer = resourceManager->bm.pin(resourceManager->fm, this->pageDirectoryId);
        pd = (PageDirectory*)this->pageDirBuffer->bufferData.data();

        PageEntry* pageEntryList = (PageEntry*)(this->pageDirBuffer->bufferData.data() + sizeof(PageDirectory));
        u32 finalPageEntryNumber = resourceManager->fm.append(filename, numOfEntries) - 1;
        TuplePage tp{ 0, resourceManager->fm.getBlockSize(), 0, resourceManager->fm.getBlockSize() };
        for (u32 i = 0; i < numOfEntries; ++i) {
          pageEntryList[i].pageNumber = lastPageNumber + 1 + i;
          pageEntryList[i].freeSpace = resourceManager->fm.getBlockSize() - sizeof(TuplePage);

          this->pageBufferId = PageId{ filename, pageEntryList[i].pageNumber };
          this->pageBuffer = resourceManager->bm.pin(resourceManager->fm, this->pageBufferId);
          this->pageBuffer->modify(&tp, sizeof(TuplePage), 0);
          this->resourceManager->bm.unpin(this->resourceManager->fm, this->pageBuffer->pageId);
        }

        currPageDir += 1;
      }
    };

    bool nextDir() {
      // i should release current page buffer since it's doesn't belong to the next dir
      if (pageBuffer) {
        resourceManager->bm.unpin(resourceManager->fm, pageBuffer->pageId);
      }

      PageDirectory* pd = reinterpret_cast<PageDirectory*>(pageDirBuffer->bufferData.data());
      if (pd->nextPage == u64Max) {
        return false;
      }
      pageDirectoryId = PageId{ filename, pd->nextPage };
      resourceManager->bm.unpin(resourceManager->fm, pageDirBuffer->pageId);
      pageDirBuffer = resourceManager->bm.pin(resourceManager->fm, pageDirectoryId);
      return true;
    };

    bool nextPageInDir() {
      const PageDirectory* pd = reinterpret_cast<PageDirectory*>(pageDirBuffer->bufferData.data());
      const PageEntry* pageEntryList = reinterpret_cast<PageEntry*>(pageDirBuffer->bufferData.data() + sizeof(PageDirectory));
      if (!pageBuffer) {
        // no page exists, so choose the first page entry
        if (pd->numberOfEntries == 0) {
          return false;
        }
        else {
          this->pageBufferId = PageId{ filename, pageEntryList[0].pageNumber };
          pageBuffer = resourceManager->bm.pin(resourceManager->fm, this->pageBufferId);
          pageEntryIndex = 0;
          return true;
        }
      }
      else {
        // page exists, so choose the next page entry
        resourceManager->bm.unpin(resourceManager->fm, pageBuffer->pageId);
        if (pageEntryIndex + 1 >= pd->numberOfEntries) {
          return false;
        }
        else {
          this->pageBufferId = PageId{ filename, pageEntryList[pageEntryIndex + 1].pageNumber };
          pageBuffer = resourceManager->bm.pin(resourceManager->fm, this->pageBufferId);
          pageEntryIndex++;
          return true;
        }
      }
    };

    /**
    Traverse to the first page where there is enough space for the record size.

    Can add new pages if there are currently unsufficient page entry/ pages.

    1. Not enough page entries in page dire -> add new page entry and new page.
    2. Not enough page directories -> add new page directory, page entry, and new page.


    1. found good page entry in current page dir
    2. page dir has space left for new page entry
    3. page dir has no space left for new page entry
      3.5 There is next page
      3.6 no next page

      return true if pages were added
      , false if no pages were added.
    */
    bool traverseFromStartTilFindSpace(u32 recordSize) {
      u32 requiredSize = recordSize;
      u32 remainingPageDirSize;
      u64 pageNumberChosen = u64Max;
      PageDirectory* pd;
      PageEntry* pageEntryList;
      do {
        pd = reinterpret_cast<PageDirectory*>(pageDirBuffer->bufferData.data());
        pageEntryList = reinterpret_cast<PageEntry*>(pageDirBuffer->bufferData.data() + sizeof(PageDirectory));
        for (u32 i = 0; i < pd->numberOfEntries; ++i) {
          if (pageEntryList[i].freeSpace >= requiredSize) {
            // unpin the current page buffer
            if (pageBuffer) {
              resourceManager->bm.unpin(resourceManager->fm, pageBuffer->pageId);
            }

            // set up the page buffer to point to the chosen page
            this->pageBufferId = PageId{ filename, pageEntryList[i].pageNumber };
            this->pageEntryIndex = i;
            pageBuffer = resourceManager->bm.pin(resourceManager->fm, this->pageBufferId);
            pageNumberChosen = pageEntryList[i].pageNumber;

            return true;
          }
        };
      } while (this->nextDir());

      if (pageBuffer) {
        resourceManager->bm.unpin(resourceManager->fm, pageBuffer->pageId);
      }

      // save current pageId
      PageId savedPageId = this->pageDirectoryId;
      this->extendHeapFile();

      // restore state
      this->resourceManager->bm.unpin(this->resourceManager->fm, this->pageDirBuffer->pageId);
      if (pageBuffer) {
        resourceManager->bm.unpin(resourceManager->fm, pageBuffer->pageId);
      }
      this->pageDirectoryId = savedPageId;
      this->pageDirBuffer = resourceManager->bm.pin(resourceManager->fm, this->pageDirectoryId);
      return this->traverseFromStartTilFindSpace(requiredSize);
    };

    bool canDirStorePageEntry() {
      const PageDirectory* pd = reinterpret_cast<PageDirectory*>(pageDirBuffer->bufferData.data());
      u32 remainingSize = resourceManager->fm.getBlockSize() - sizeof(PageDirectory) - (pd->numberOfEntries * sizeof(PageEntry));
      return remainingSize >= sizeof(PageEntry);
    }
  };
  // create new heap file
  void createHeapFile(ResourceManager& rm, std::string filename);
  void insertTuple(HeapFile::HeapFileIterator& iter, const Tuple& tuple);
  void insertTuples(HeapFile::HeapFileIterator& iter, std::vector<Tuple>& tuples);
  void insertTuples(std::shared_ptr<ResourceManager>& rm, const std::string& filename, std::vector<Tuple>& tuples);

};