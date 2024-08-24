
#include "buffer.h"

std::fstream& FileManager::seekFile(PageId pageId) {
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

u32 FileManager::getNumberOfPages(PageId pageId) {
  return this->getNumberOfPages(pageId.filename);
}

u32 FileManager::getNumberOfPages(std::string filename) {
  u32 end = std::filesystem::file_size(filename);
  return end / blockSize;
}


bool FileManager::read(PageId pageId, std::vector<char>& bufferData) {
  if (pageId.pageNumber >= getNumberOfPages(pageId)) {
    return false;
  }

  auto& fileStream = seekFile(pageId);

  fileStream.read(bufferData.data(), blockSize);
  if (fileStream.fail()) {
    throw std::runtime_error("Error reading file");
  }

  return true;
}

bool FileManager::write(PageId pageId, std::vector<char>& bufferData) {
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

u32 FileManager::append(std::string filename, int numberOfBlocksToAppend) {
  if (fileMap.find(filename) == fileMap.end()) {
    fileMap.insert({ filename, std::fstream(filename, std::ios::in | std::ios::out | std::ios::binary) });
  }

  //u32 initialFileSize = getNumberOfPages(filename);

  auto& fileStream = fileMap.at(filename);
  std::vector<char> emptyBufferData(blockSize, 0);

  // Seek to the end of the file
  fileStream.seekp(0, std::ios::end);

  // Write the data to the end of the file
  while (numberOfBlocksToAppend > 0) {
    fileStream.write(emptyBufferData.data(), emptyBufferData.size());
    numberOfBlocksToAppend--;
  }

  fileStream.flush();
  if (!fileStream) {
    throw std::runtime_error("Error appending to file");
  }

  u32 end = std::filesystem::file_size(filename);
  //u32 finalFileSize = getNumberOfPages(filename);
  return getNumberOfPages(filename) - 1;
}

void FileManager::createFileIfNotExists(const std::string& fileName) {
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

bool FileManager::doesFileExists(const std::string& fileName)
{
  return std::filesystem::exists(fileName);
}

bool FileManager::deleteFile(const std::string& fileName)
{
  if (this->fileMap.find(fileName) != this->fileMap.end()) {
    this->fileMap.at(fileName).close();
    this->fileMap.erase(fileName);
  }


  if (std::filesystem::remove(fileName) != 0) {
    std::cerr << "Error deleting file " << fileName << std::endl;
    return false;
  }
  else {
    std::cout << "File successfully deleted " << fileName << std::endl;
    return true;
  }
}

void HeapFile::createHeapFile(ResourceManager& rm, std::string filename, const u32 newPages) {
  auto& fm = rm.fm;
  auto& bm = rm.bm;

  // create a new heap file.
  PageId pageId{ filename, 0 };
  fm.createFileIfNotExists(pageId.filename);
  fm.append(filename, newPages + 1);

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


  // Add tuple header for each page 
  TuplePage tp{ 0, fm.getBlockSize(), 0, fm.getBlockSize() };
  for (u64 i = 1; i <= newPages; ++i) {
    PageId tuplePageId{ filename, i };
    BufferFrame* tuplePageBuffer = bm.pin(fm, tuplePageId);
    tuplePageBuffer->modify(&tp, sizeof(TuplePage), 0);
    bm.unpin(fm, tuplePageId);
  }
}

PageId HeapFile::appendHeapFilePageDirectory(ResourceManager& rm, std::string filename) {
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

  // previous last page directory
  // set next pointer to the new page id created.
  u32 lastPageNumber = fm.append(currentPageId.filename) - 1;
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

PageId HeapFile::appendNewHeapPage(ResourceManager& rm, std::string filename) {
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

  u32 lastPageNumber = fm.append(currentPageId.filename);

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

void HeapFile::insertTuple(HeapFile::HeapFileIterator& iter, const Tuple& tuple) {
  //auto nametuple = tuple.fields[0]->getConstant().str;
  //if (nametuple == "Julian" || nametuple == "Skylar") {
  //  int a = 0;
  //}
  iter.traverseFromStartTilFindSpace(tuple.recordSize);
  // Decrease page entry free space size
  BufferFrame* directoryFrame = iter.getPageDirBuffer();
  PageEntry* pageEntryList = reinterpret_cast<PageEntry*>(directoryFrame->bufferData.data() + sizeof(PageDirectory));
  pageEntryList[iter.getPageEntryIndex()].freeSpace -= tuple.recordSize;
  directoryFrame->dirty = true;

  // Get the tuple page headers.
  BufferFrame* tupleFrame = iter.getPageBuffer();
  TuplePage* pe = reinterpret_cast<TuplePage*>(tupleFrame->bufferData.data());
  Slot* slot = reinterpret_cast<Slot*>(tupleFrame->bufferData.data() + sizeof(TuplePage));
  u32 numberOfSlots = pe->numberOfSlots;

  // find an empty slot
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
    pageEntryList[iter.getPageEntryIndex()].freeSpace -= sizeof(Slot);
    directoryFrame->dirty = true;
  }

  // Set current slot to be occupied
  auto& currSlot = slot[emptySlotIdx];
  u32 prevSize = pe->lastOccupiedPosition;
  u32 offset = pe->lastOccupiedPosition - tuple.recordSize;
  pe->lastOccupiedPosition -= tuple.recordSize;
  currSlot.setOccupied(true);
  currSlot.setOffset(offset);

  // Write the tuple to the buffer
  for (auto& field : tuple.fields) {
    field->write(tupleFrame->bufferData.data(), offset);
    offset += field->getLength();
  }
  tupleFrame->dirty = true;
}

void HeapFile::insertTuples(HeapFile::HeapFileIterator& iter, std::vector<Tuple>& tuples) {
  std::sort(begin(tuples), end(tuples), [](auto& lhs, auto& rhs) {
    return lhs.recordSize < rhs.recordSize;
    });

  for (auto& tuple : tuples) {
    HeapFile::insertTuple(iter, tuple);
  }
}

void HeapFile::insertTuples(std::shared_ptr<ResourceManager>& rm, const std::string& filename, std::vector<Tuple>& tuples) {
  HeapFile::HeapFileIterator iter(filename, rm);
  iter.findFirstDir();
  insertTuples(iter, tuples);
}



void HeapFile::insertTuple(ResourceManager& rm, const std::string& filename, Tuple& tuple) {
  auto& fm = rm.fm;
  auto& bm = rm.bm;

  PageId pageId{ filename, 0 };
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
    PageId newPage = appendNewHeapPage(rm, filename);
    pageNumberChosen = newPage.pageNumber;
  }

  bm.unpin(fm, pageId);

  // Get the headers
  PageId tuplePageId{ filename, pageNumberChosen };
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
}