#include "catch.hpp"

#include "../src/buffer.h"
#include <vector>
#include <iostream>
#include <fstream>
#include <filesystem>

struct DeferDeleteFile {
  std::string fileName;
  DeferDeleteFile(const std::string& fileName) : fileName(fileName) {};
  ~DeferDeleteFile() {
    if (std::filesystem::remove(fileName) != 0) {
      std::cerr << "Error deleting file" << std::endl;
    }
    else {
      std::cout << "File successfully deleted" << std::endl;
    }
  }
};

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

TEST_CASE("Insert tuple") {
  const std::string fileName = "testFile";
  DeferDeleteFile deferDeleteFile(fileName);
  {
    std::shared_ptr<ResourceManager> rm = std::make_shared<ResourceManager>(PAGE_SIZE_M, 10);

    HeapFileOps::createTable(*rm, fileName);

    Schema schema(fileName, {});
    std::vector<WriteField*> fields{ new VarCharField("memes lol haha"), new FixedCharField(20, "fixed char"), new IntField(4) };
    Tuple tuple(fields);

    HeapFileOps::insertTuple(*rm, schema, tuple);
    std::vector<std::unique_ptr<ReadField>> readFields;
    readFields.emplace_back(std::make_unique<ReadVarCharField>());
    readFields.emplace_back(std::make_unique<ReadFixedCharField>(20));
    readFields.emplace_back(std::make_unique<ReadIntField>());

    HeapFile::Scan scan(fileName, rm, std::move(readFields));
    scan.getFirst();
    while (scan.next()) {
      auto v = scan.get();
      int a = 1;
    };
  }
}