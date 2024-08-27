#pragma once

#include "../common.h"
#include <unordered_map>
#include <string>
#include <fstream>
#include "../buffer.h"

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
