#pragma once

#include "common.h"
#include "query.h"
#include "buffer.h"
#include <string>

namespace HeapFile {
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

    void print(u32 pageId, PageType pageType);
    void createHeapFile(ResourceManager& rm, std::string filename);
  public:
    HeapFileIterator(std::string filename, std::shared_ptr<ResourceManager> rm);

    ~HeapFileIterator();

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
    bool findFirstDir();

    void extendHeapFile();

    bool nextDir();

    bool nextPageInDir();

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
    bool traverseFromStartTilFindSpace(u32 recordSize);

    bool canDirStorePageEntry();

    void insertTuple(const Tuple& tuple);

    void insertTuples(std::vector<Tuple>& tuples);
  };
};