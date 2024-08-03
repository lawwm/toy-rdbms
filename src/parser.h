
#pragma once

#include <string>
#include <unordered_map>
enum TokenType {
  // Single character tokens
  SEMI_COLON,
  LEFT_PAREN,
  RIGHT_PAREN,
  COMMA, DOT, MINUS, PLUS, STAR, SLASH, EQUAL, 

  // compararison operators
  NOT_EQUAL,GREATER, GREATER_EQUAL, LESS, LESS_EQUAL,

  // literals
  IDENTIFIER, STRING, NUMBER,

  // keywords
  SELECT, AS, FROM, WHERE, AND, OR, IS, NOT, NULL_TOKEN, JOIN, ON,
  
  // error
  ERROR
};

struct Token { //12 bytes???
  TokenType type;
  int line;
  std::string lexeme;
};

class Lexer {
private:
  std::string input;
  int idx = 0;
  int line = 0;

  void skipWhiteSpace();
  std::tuple<Token, int> scanToken();
public:

  Lexer(std::string input) : input{ input }, idx(0) {};
  Lexer(std::string input, int idx) : input(input), idx(idx) {};

  Token nextToken();
  Token matchToken(TokenType type);
  Token peekToken();
  bool isEOF();
};
