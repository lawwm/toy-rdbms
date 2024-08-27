#include "FileManager.h"


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