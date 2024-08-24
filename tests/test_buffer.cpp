#include "catch.hpp"

#include <vector>
#include <iostream>
#include <fstream>
#include <filesystem>
#include "./test_utils.h"
#include "../src/buffer.h"

TEST_CASE("Check buffer works") {

  const std::string fileName = "testbuffer12345";
  DeferDeleteFile deferDeleteFile(fileName);
  const size_t bufferSize = 4096; // 4 KB

  // Create or truncate the file to ensure it has the correct size (4096 * 5 bytes)
  std::ofstream ofs(fileName, std::ios::out | std::ios::binary | std::ios::trunc);
  ofs.seekp(bufferSize * 5 - 1);
  ofs.write("", 1);
  ofs.close();

  FileManager fileManager(bufferSize);
  std::vector<char> writeTest(bufferSize);
  std::string testString = "Hello, world!";
  int testInt = 1234567;
  int offset = 50;
  for (int i = 0; i < testString.size(); i++) {
    writeTest[i] = testString[i % testString.size()];
  }
  std::memcpy(writeTest.data() + offset, &testInt, sizeof(int));

  fileManager.write(PageId{ fileName, 2 }, writeTest);

  std::vector<char> readTest(bufferSize);
  fileManager.read(PageId{ fileName, 2 }, readTest);


  for (int i = 0; i < testString.size(); i++) {
    REQUIRE(writeTest[i] == readTest[i]);
  }
  int readInt;
  std::memcpy(&readInt, readTest.data() + offset, sizeof(int));
  REQUIRE(readInt == testInt);
}

TEST_CASE("CreateHeapFile works") {
  const std::string fileName = "testbuffer12345";
  DeferDeleteFile deferDeleteFile(fileName);

  std::shared_ptr<ResourceManager> rm = std::make_shared<ResourceManager>(TEST_PAGE_SIZE, 10);
  HeapFile::createHeapFile(*rm, fileName, 8);

  u32 numPages = rm->fm.getNumberOfPages(fileName);
  REQUIRE(numPages == 9);

  u32 currPage = 0;
  for (; currPage < 9; ++currPage) {
    BufferFrame* bf = rm->bm.pin((rm->fm), PageId{ fileName, currPage });
    PageType* pt = (PageType*)bf->bufferData.data();
    if (currPage == 0) {
      REQUIRE(*pt == PageType::DirectoryPage);
    }
    else {
      REQUIRE(*pt == PageType::TuplePage);
    }
    rm->bm.unpin(rm->fm, PageId{ fileName, currPage });
  }

  for (; currPage <= 14; ++currPage) {
    PageId pageId = HeapFile::appendNewHeapPage(*rm, fileName);
    BufferFrame* bf = rm->bm.pin((rm->fm), pageId);
    PageType* pt = (PageType*)bf->bufferData.data();
    REQUIRE(*pt == PageType::TuplePage);
    rm->bm.unpin(rm->fm, pageId);
    REQUIRE(pageId.pageNumber == currPage);
  }

  numPages = rm->fm.getNumberOfPages(fileName);
  REQUIRE(numPages == 15);
}