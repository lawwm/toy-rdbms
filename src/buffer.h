#pragma once

#include <string>
#include <vector>
#include <iostream>
#include <fstream>
#include <cstring>
#include <stdexcept>
#include <unordered_map>
#include <cstdint>
#include <filesystem>

// this page stores the storage engine.

using u8 = std::uint8_t;
using u32 = std::uint32_t;
using u64 = std::uint64_t;
using i8 = std::int8_t;
using i16 = std::int16_t;
using i32 = std::int32_t;


const static u8 u8Max = std::numeric_limits<u8>::max();
const static u32 u32Max = std::numeric_limits<u32>::max();
const static u64 u64Max = std::numeric_limits<u64>::max();



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
  char tableName[128];

  PageDirectory(u64 nextPage, u64 prevPage, u64 numberOfEntries) :
    pageType{ PageType::DirectoryPage },
    nextPage{ nextPage }, prevPage{ prevPage }, numberOfEntries{ numberOfEntries } {}
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

class WriteField {
public:
  virtual ~WriteField() = default;
  virtual u32 getLength() = 0;
  virtual void write(char* buffer, u32 offset) = 0;
};
class ReadField {
public:
  virtual ~ReadField() = default;
  virtual WriteField* get(const char* buffer, u32 offset) = 0;
};


// first 4 bytes is string length, then the string
class VarCharField : public WriteField {
  std::string value;
public:
  virtual ~VarCharField() override = default;
  VarCharField(std::string value) : value{ value } {}

  virtual u32 getLength() override {
    return value.size() + sizeof(u32);
  }
  virtual void write(char* buffer, u32 offset) override {
    u32 length = value.size();
    std::memcpy(buffer + offset, &length, sizeof(u32));
    std::memcpy(buffer + offset + sizeof(u32), value.c_str(), value.size());
  }
};

class ReadVarCharField : public ReadField {
public:
  ReadVarCharField() {}
  virtual ~ReadVarCharField() override = default;

  virtual WriteField* get(const char* buffer, u32 offset) override {
    u32 length;
    std::memcpy(&length, buffer + offset, sizeof(u32));
    std::string value(buffer + offset + sizeof(u32), length);
    return new VarCharField(value);
  }
};

class FixedCharField : public WriteField {
private:
  int length;
  std::string value;
public:
  FixedCharField(int length, std::string value) : length{ length }, value{ value } {}
  virtual ~FixedCharField() override = default;

  virtual u32 getLength() override {
    return length;
  }
  virtual void write(char* buffer, u32 offset) override {
    std::memcpy(buffer + offset, value.c_str(), value.size());
  }
};

class ReadFixedCharField : public ReadField {
private:
  int length;
public:
  ReadFixedCharField(int length) : length{ length } {}
  virtual ~ReadFixedCharField() override = default;

  virtual WriteField* get(const char* buffer, u32 offset) override {
    std::string value(buffer + offset + sizeof(u32), length);
    return new FixedCharField(length, value);
  }
};

class IntField : public WriteField {
private:
  i32 value;
public:
  IntField(i32 value) : value{ value } {}
  virtual ~IntField() override = default;

  virtual u32 getLength() override {
    return sizeof(i32);
  }
  virtual void write(char* buffer, u32 offset) override {
    std::memcpy(buffer + offset, &value, sizeof(i32));
  }
};

class ReadIntField : public ReadField {
public:
  ReadIntField() {}
  virtual ~ReadIntField() override = default;

  virtual WriteField* get(const char* buffer, u32 offset) override {
    i32 value;
    std::memcpy(&value, buffer + offset, sizeof(i32));
    return new IntField(value);
  }
};

struct Tuple {
  std::vector<std::unique_ptr<WriteField>> fields;
  u32 recordSize;
  Tuple(std::vector<std::unique_ptr<WriteField>> inputFields) : fields{ std::move(inputFields) } {
    recordSize = 0;
    for (auto& field : this->fields) {
      recordSize += field->getLength();
    }
  }
};

struct Schema {
  std::string filename;
  std::vector<ReadField*> fields;

  Schema(std::string filename, std::vector<ReadField*> fields) : filename{ filename }, fields{ fields } {};
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

  std::fstream& seekFile(PageId pageId) {
    long offset = pageId.pageNumber * blockSize;

    if (fileMap.find(pageId.filename) == fileMap.end()) {
      fileMap.insert({ pageId.filename, std::fstream(pageId.filename, std::ios::in | std::ios::out | std::ios::binary) });
    }


    std::fstream& fileStream = fileMap.at(pageId.filename);
    if (!fileStream && fileStream.eof()) {
      fileStream.clear();
    }

    fileStream.seekg(offset, std::ios::beg);
    return fileStream;
  }

  u32 getNumberOfPages(PageId pageId) {
    u32 end = std::filesystem::file_size(pageId.filename);
    return end / blockSize;
  }

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

  bool read(PageId pageId, std::vector<char>& bufferData) {
    if (pageId.pageNumber >= getNumberOfPages(pageId)) {
      return false;
    }

    auto& fileStream = seekFile(pageId);

    fileStream.read(bufferData.data(), blockSize);
    if (fileStream.fail()) {
      throw std::runtime_error("Error reading file");
    }

    return true;
  };

  bool write(PageId pageId, std::vector<char>& bufferData) {
    if (pageId.pageNumber >= getNumberOfPages(pageId)) {
      return false;
    }


    auto& fileStream = seekFile(pageId);

    fileStream.write(bufferData.data(), blockSize);
    if (fileStream.fail()) {
      throw std::runtime_error("Error writing to file");
    }

    return true;
  };

  // return the last pageId
  u32 append(PageId fileinfo, int numberOfBlocksToAppend = 1) {
    if (fileMap.find(fileinfo.filename) == fileMap.end()) {
      fileMap.insert({ fileinfo.filename, std::fstream(fileinfo.filename, std::ios::in | std::ios::out | std::ios::binary) });
    }

    auto& fileStream = fileMap.at(fileinfo.filename);
    std::vector<char> emptyBufferData(blockSize, 0);

    // Seek to the end of the file
    fileStream.seekp(0, std::ios::end);

    // Write the data to the end of the file
    while (numberOfBlocksToAppend > 0) {
      fileStream.write(emptyBufferData.data(), emptyBufferData.size());
      numberOfBlocksToAppend--;
    }

    if (!fileStream) {
      throw std::runtime_error("Error appending to file");
    }

    u32 end = std::filesystem::file_size(fileinfo.filename);

    return end / blockSize;
  }

  u32 getNumberOfPages(std::string filename) {
    u32 end = std::filesystem::file_size(filename);
    return end / blockSize;
  }

  void createFileIfNotExists(const std::string& fileName) {
    // Check if the file already exists
    std::ifstream infile(fileName, std::ios::binary);
    if (infile.is_open()) {
      infile.close();
      return;
    }

    // Create the file
    std::ofstream outfile(fileName, std::ios::binary);
    if (!outfile.is_open()) {
      throw std::runtime_error("Error creating file");
    }

    outfile.close();
  }
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

  // create new heap file
  void createHeapFile(ResourceManager& rm, std::string filename, const u32 newPages = 8) {
    auto& fm = rm.fm;
    auto& bm = rm.bm;

    // create a new heap file.
    PageId pageId{ filename, 0 };
    fm.createFileIfNotExists(pageId.filename);
    fm.append(pageId, newPages + 1);

    // Add page directory header to the page directory
    BufferFrame* bf = bm.pin(fm, pageId);
    PageDirectory pd{ u64Max, u64Max, newPages };
    std::strncpy(pd.tableName, filename.c_str(), 128);

    // Add page entries to the page directory
    std::vector<PageEntry> pe;
    for (u64 i = 1; i <= newPages; ++i) {
      u32 freeSpace = fm.getBlockSize() - ((u32)sizeof(TuplePage));
      pe.push_back(PageEntry{ i, freeSpace });
    }

    // flush into page directory
    bf->modify(&pd, sizeof(PageDirectory), 0);
    bf->modify(pe.data(), sizeof(PageEntry) * pe.size(), sizeof(PageDirectory));
    bm.unpin(fm, pageId);


    // Add tuple header for each page directory
    TuplePage tp{ 0, fm.getBlockSize(), 0, fm.getBlockSize() };
    for (u64 i = 1; i <= newPages; ++i) {
      PageId tuplePageId{ filename, i };
      BufferFrame* tuplePageBuffer = bm.pin(fm, tuplePageId);
      tuplePageBuffer->modify(&tp, sizeof(TuplePage), 0);
      bm.unpin(fm, tuplePageId);
    }
  }

  // add new heap file directory
  PageId appendHeapFilePageDirectory(ResourceManager& rm, std::string filename) {
    auto& fm = rm.fm;
    auto& bm = rm.bm;

    // create a new heap file.
    PageId currentPageId{ filename, 0 };

    // Add page directory header to the page directory
    BufferFrame* bf = bm.pin(fm, currentPageId);
    PageDirectory* pd = (PageDirectory*)bf->bufferData.data();
    while (pd->nextPage != u64Max) {// Find the last page directory
      u64 nextPageIdPageNum = pd->nextPage;
      PageId nextPageId{ filename, nextPageIdPageNum };
      bm.unpin(fm, currentPageId);

      currentPageId = nextPageId;
      bf = bm.pin(fm, currentPageId);
      pd = (PageDirectory*)bf->bufferData.data();
    }

    // append a new page
    u32 lastPageNumber = fm.append(currentPageId) - 1;
    pd->nextPage = lastPageNumber;
    bf->modify(&pd, sizeof(PageDirectory), 0);
    bm.unpin(fm, currentPageId);

    // create a new page directory
    PageDirectory newPd{ currentPageId.pageNumber, u64Max, 0 };
    std::strncpy(newPd.tableName, filename.c_str(), 128);
    BufferFrame* newFrame = bm.pin(fm, currentPageId);
    newFrame->modify(&newPd, sizeof(PageDirectory), 0);
    bm.unpin(fm, currentPageId);

    return PageId{ filename, lastPageNumber };
  }

  // add new heap page
  PageId appendNewHeapPage(ResourceManager& rm, std::string filename) {
    auto& fm = rm.fm;
    auto& bm = rm.bm;

    // create a new heap file.
    PageId currentPageId{ filename, 0 };

    // Add page directory header to the page directory
    BufferFrame* bf = bm.pin(fm, currentPageId);
    PageDirectory* pd = (PageDirectory*)bf->bufferData.data();
    bool shouldCreateNewHeapDir = false;
    while (true) {
      u64 numEntries = pd->numberOfEntries;
      u32 blockSize = fm.getBlockSize();
      u32 remainingSize = blockSize - sizeof(PageDirectory) - (numEntries * sizeof(PageEntry));

      // current page directory has enough space for page entry
      if (sizeof(PageEntry) <= remainingSize) {
        break;
      }
      // need a new page directory since all current directories do not have enough space.
      else if (pd->nextPage == u64Max) {
        shouldCreateNewHeapDir = true;
        break;
      }
      // go to next page directory
      else {
        u64 nextPageIdPageNum = pd->nextPage;
        PageId nextPageId{ filename, nextPageIdPageNum };
        bm.unpin(fm, currentPageId);

        currentPageId = nextPageId;
        bf = bm.pin(fm, currentPageId);
        pd = (PageDirectory*)bf->bufferData.data();
      }
    };

    if (shouldCreateNewHeapDir) {
      PageId newPageId = appendHeapFilePageDirectory(rm, filename);
      bm.unpin(fm, currentPageId);
      currentPageId = newPageId;
      bf = bm.pin(fm, currentPageId);
      pd = (PageDirectory*)bf->bufferData.data();
    }

    u32 lastPageNumber = fm.append(currentPageId) - 1;

    // Add page entry to the page directory
    u32 freeSpace = fm.getBlockSize() - ((u32)sizeof(TuplePage));
    PageEntry newPageEntry{ lastPageNumber, freeSpace };
    u32 offset = sizeof(PageDirectory) + pd->numberOfEntries * sizeof(PageEntry);
    pd->numberOfEntries++;
    bf->modify(&newPageEntry, sizeof(PageEntry), offset);
    bm.unpin(fm, currentPageId);

    // Add tuple header to page
    PageId tuplePageId{ filename, lastPageNumber };
    TuplePage tp{ 0, fm.getBlockSize(), 0, fm.getBlockSize() };
    BufferFrame* tuplePageBuffer = bm.pin(fm, PageId{ filename, lastPageNumber });
    tuplePageBuffer->modify(&tp, sizeof(TuplePage), 0);
    bm.unpin(fm, tuplePageId);

    return tuplePageId;
  }


  void insertTuple(ResourceManager& rm, Schema& schema, Tuple& tuple) {
    auto& fm = rm.fm;
    auto& bm = rm.bm;

    PageId pageId{ schema.filename, 0 };
    BufferFrame* bf = bm.pin(fm, pageId);

    PageDirectory* pd = (PageDirectory*)bf->bufferData.data();
    u64 numberOfEntries = pd->numberOfEntries;

    // Choose a page that has sufficient space.
    u64 pageNumberChosen = -1;
    const PageEntry* pageEntryList = reinterpret_cast<PageEntry*>(bf->bufferData.data() + sizeof(PageDirectory));
    for (int i = 0; i < numberOfEntries; ++i)
    {
      auto& pageEntry = pageEntryList[i];
      // if the page has enough space for the tuple
      if (pageEntry.freeSpace >= tuple.recordSize + sizeof(Slot))
      {
        pageNumberChosen = pageEntry.pageNumber;
        break;
      }
    };

    if (pageNumberChosen == -1)
    {
      // if no space for tuple
      PageId newPage = appendNewHeapPage(rm, schema.filename);
      pageNumberChosen = newPage.pageNumber;
    }

    bm.unpin(fm, pageId);

    // Get the headers
    PageId tuplePageId{ schema.filename, pageNumberChosen };
    BufferFrame* tupleFrame = bm.pin(fm, tuplePageId);
    TuplePage* pe = reinterpret_cast<TuplePage*>(tupleFrame->bufferData.data());
    Slot* slot = reinterpret_cast<Slot*>(tupleFrame->bufferData.data() + sizeof(TuplePage));
    u32 numberOfSlots = pe->numberOfSlots;

    u32 emptySlotIdx = u32Max;
    for (u32 i = 0; i < numberOfSlots; ++i) {
      if (!slot[i].isOccupied()) {
        emptySlotIdx = i;
        break;
      }
    }

    if (emptySlotIdx == u32Max) {
      emptySlotIdx = numberOfSlots;
      pe->numberOfSlots += 1;
    }

    // Set current slot to be occupied
    auto& currSlot = slot[emptySlotIdx];
    u32 offset = pe->lastOccupiedPosition - tuple.recordSize;
    currSlot.setOccupied(true);
    currSlot.setOffset(offset);

    // Write the tuple to the buffer
    for (auto& field : tuple.fields) {
      field->write(tupleFrame->bufferData.data(), offset);
      offset += field->getLength();
    }
    pe->lastOccupiedPosition = offset;
    tupleFrame->dirty = true;
    bm.unpin(fm, tuplePageId);
  };


  class Scan {
    PageId currentPageId;
    i32 currentSlot;
    std::shared_ptr<ResourceManager> rm;
    BufferFrame* currBuffer;
    std::string filename;
    std::vector<std::unique_ptr<ReadField>> input;

  public:
    using value_type = std::vector<WriteField*>;

    Scan(std::string filename, std::shared_ptr<ResourceManager> rm, std::vector<std::unique_ptr<ReadField>>&& input)
      : currentPageId{ PageId{ filename, 0 } }, currentSlot{ -1 },
      rm{ rm }, currBuffer{ nullptr }, filename{ filename }, input{ std::move(input) } {
    }

    value_type get() {
      Slot* slot = reinterpret_cast<Slot*>(currBuffer->bufferData.data() + sizeof(TuplePage));

      std::vector<WriteField*> output;
      u32 offset = slot[currentSlot].getOffset();
      for (int i = 0; i < input.size(); ++i) {
        WriteField* wf = input[i]->get(currBuffer->bufferData.data(), offset);
        offset += wf->getLength();
        output.push_back(wf);
      }
      return output;
    }

    bool getFirst() {
      currentPageId = PageId{ currentPageId.filename, 0 };
      currBuffer = rm->bm.pin(rm->fm, currentPageId);
      return true;
    }

    bool findNextPage() {
      rm->bm.unpin(rm->fm, this->currentPageId);
      this->currentPageId.pageNumber += 1;

      while (true) {
        this->currBuffer = rm->bm.pin(rm->fm, this->currentPageId);
        if (!this->currBuffer) break;

        this->currentSlot = -1;
        PageType* pt = (PageType*)currBuffer->bufferData.data();
        if (*pt == PageType::TuplePage) {
          return true;
        }
        else {
          rm->bm.unpin(rm->fm, this->currentPageId);
          this->currentPageId.pageNumber += 1;
        }
      }

      return false;
    }

    bool next() {

      while (true) {
        TuplePage* pe = reinterpret_cast<TuplePage*>(currBuffer->bufferData.data());
        if (pe->pageType != PageType::TuplePage) {
          bool foundNextPage = findNextPage();
          if (!foundNextPage) {
            return false;
          }
          else {
            continue;
          }
        }
        u32 numberOfSlots = pe->numberOfSlots;
        i32 nextSlot = currentSlot + 1;
        if (nextSlot < numberOfSlots) {
          currentSlot = nextSlot;
          return true;
        }
        else { // no more slots left
          bool foundNextPage = findNextPage();
          if (!foundNextPage) {
            return false;
          }

        }
      }
    }
  };
  // iterate through non-empty heap pages
}