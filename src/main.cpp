// newsql.cpp : Defines the entry point for the application.
//

#include <iostream>

#include "parser.h"

int main()
{
  std::cout << "Hello CMake." << std::endl;
  Lexer lexer("SELECT * FROM table;");
  lexer.nextToken();
  return 0;
}
