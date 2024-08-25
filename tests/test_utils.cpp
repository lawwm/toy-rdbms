#include "test_utils.h"

#include <fstream>
#include <sstream>
#include <filesystem>

DeferDeleteFile::~DeferDeleteFile() {
  for (auto& fileName : fileNames) {
    if (std::filesystem::remove(fileName) != 0) {
      std::cerr << "Error deleting file " << fileName << std::endl;
    }
    else {
      std::cout << "File successfully deleted " << fileName << std::endl;
    }
  }
}

std::string readFileAsString(const std::string& filePath) {
  std::ifstream file(filePath);  // Open the file
  if (!file.is_open()) {
    throw std::runtime_error("Could not open file");
  }

  std::stringstream buffer;
  buffer << file.rdbuf();  // Read file contents into the stringstream buffer

  return buffer.str();  // Convert buffer into a string and return
}

