#pragma once

#include "../src/parser.h"
#include <filesystem>

struct DeferDeleteFile {
  std::vector<std::string> fileNames;
  DeferDeleteFile(const std::string& fileName) : fileNames({ fileName }) {};
  DeferDeleteFile(const std::initializer_list<std::string>& fileNames) : fileNames(fileNames) {};
  ~DeferDeleteFile();
};

// convenience functions for creating tokens

static Token ttoken(std::string lexeme) {
  return Token{ STRING, 0, lexeme };
}

static Token ttoken(int number) {
  return Token{ NUMBER, 0, "", number };
}