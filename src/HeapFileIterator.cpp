#include "HeapFileIterator.h"
#include <string>

HeapFile::HeapFileIterator::HeapFileIterator(std::string filename, std::shared_ptr<ResourceManager> rm) : pageBuffer{ nullptr },
pageEntryIndex{ u32Max }, filename{ filename }, resourceManager{ rm } {
  pageDirectoryId = PageId{ filename, 0 };
  pageDirBuffer = resourceManager->bm.pin(resourceManager->fm, pageDirectoryId);
};

HeapFile::HeapFileIterator::~HeapFileIterator() {
  if (resourceManager) {
    resourceManager->bm.unpin(resourceManager->fm, pageDirectoryId);
    if (pageBuffer) {
      resourceManager->bm.unpin(resourceManager->fm, pageBuffer->pageId);
    }
  }
};


bool HeapFile::HeapFileIterator::findFirstDir() {
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

void HeapFile::HeapFileIterator::extendHeapFile() {
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
    //u32 finalPageEntryNumber = resourceManager->fm.append(filename, numOfEntries) - 1;
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
}


bool HeapFile::HeapFileIterator::nextDir() {
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


bool HeapFile::HeapFileIterator::nextPageInDir() {
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

bool HeapFile::HeapFileIterator::traverseFromStartTilFindSpace(u32 recordSize) {
  u32 requiredSize = recordSize;
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

bool HeapFile::HeapFileIterator::canDirStorePageEntry() {
  const PageDirectory* pd = reinterpret_cast<PageDirectory*>(pageDirBuffer->bufferData.data());
  u32 remainingSize = resourceManager->fm.getBlockSize() - sizeof(PageDirectory) - (pd->numberOfEntries * sizeof(PageEntry));
  return remainingSize >= sizeof(PageEntry);
}


void HeapFile::createHeapFile(ResourceManager& rm, std::string filename) {
  auto& fm = rm.fm;
  auto& bm = rm.bm;

  // create a new heap file.
  fm.createFileIfNotExists(filename);
  u32 lastPageNumber = fm.append(filename, 1);
  u64 numOfEntries = (fm.getBlockSize() - sizeof(PageDirectory)) / sizeof(PageEntry);

  // Add page directory header to the page directory
  PageId pageId{ filename, 0 };
  BufferFrame* bf = bm.pin(fm, pageId);
  PageDirectory pd{ u64Max, u64Max, numOfEntries, 1, filename };
  bf->modify(&pd, sizeof(PageDirectory), 0);

  // Add page entries to the page directory
  TuplePage tp{ 0, fm.getBlockSize(), 0, fm.getBlockSize() };
  PageEntry* pageEntryList = (PageEntry*)(bf->bufferData.data() + sizeof(PageDirectory));
  u32 finalPageEntryNumber = fm.append(filename, numOfEntries) - 1;

  for (u64 i = 0; i < numOfEntries; ++i) {
    pageEntryList[i].pageNumber = lastPageNumber + 1 + i;
    pageEntryList[i].freeSpace = fm.getBlockSize() - sizeof(TuplePage);

    // Add tuple header for each page
    PageId tuplePageId{ filename, pageEntryList[i].pageNumber };
    BufferFrame* tuplePageBuffer = bm.pin(fm, tuplePageId);
    tuplePageBuffer->modify(&tp, sizeof(TuplePage), 0);
    bm.unpin(fm, tuplePageId);
  }

  // Clear the directory buffer
  bm.unpin(fm, pageId);
}

void HeapFile::insertTuple(HeapFile::HeapFileIterator& iter, const Tuple& tuple) {
  iter.traverseFromStartTilFindSpace(tuple.recordSize + sizeof(Slot));
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

