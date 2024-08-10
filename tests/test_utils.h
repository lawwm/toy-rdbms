#pragma once

#include "../src/parser.h"

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

// convenience functions for creating tokens

static Token ttoken(std::string lexeme) {
  return Token{ STRING, 0, lexeme };
}

static Token ttoken(int number) {
  return Token{ NUMBER, 0, "", number };
}