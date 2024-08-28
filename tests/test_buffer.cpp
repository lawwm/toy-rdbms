#include "catch.hpp"

#include <vector>
#include <iostream>
#include <fstream>
#include <filesystem>
#include "./test_utils.h"
#include "../src/HeapFileIterator.h"

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

  u32 initialTuplePages = 8;
  u32 addedTuplePages = 100;

  std::shared_ptr<ResourceManager> rm = std::make_shared<ResourceManager>(TEST_PAGE_SIZE, 10);
  HeapFile::HeapFileIterator iter(fileName, rm);

  u32 numOfEntryList = (rm->fm.getBlockSize() - sizeof(PageDirectory)) / sizeof(PageEntry);
  u32 numPages = rm->fm.getNumberOfPages(fileName);

  u32 DIRWITHPAGES = numOfEntryList + 1;
  REQUIRE(numPages == DIRWITHPAGES);

  u32 count = DIRWITHPAGES;
  u32 power = 2;
  for (int i = 0; i < 4; ++i) {
    iter.extendHeapFile();
    count = (power * DIRWITHPAGES);
    power *= 2;
    numPages = rm->fm.getNumberOfPages(fileName);

    REQUIRE(numPages == count);
  }
}

