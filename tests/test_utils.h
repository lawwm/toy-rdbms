#pragma once

#include "../src/parser.h"

struct DeferDeleteFile {
  std::vector<std::string> fileNames;
  DeferDeleteFile(const std::string& fileName) : fileNames({ fileName }) {};
  DeferDeleteFile(const std::initializer_list<std::string>& fileNames) : fileNames(fileNames) {};
  ~DeferDeleteFile() {
    for (auto& fileName : fileNames) {
      if (std::filesystem::remove(fileName) != 0) {
        std::cerr << "Error deleting file " << fileName << std::endl;
      }
      else {
        std::cout << "File successfully deleted " << fileName << std::endl;
      }
    }
  }
};

// convenience functions for creating tokens

static Token ttoken(std::string lexeme) {
  return Token{ STRING, 0, lexeme };
}

static Token ttoken(int number) {
  return Token{ NUMBER, 0, "", number };
}