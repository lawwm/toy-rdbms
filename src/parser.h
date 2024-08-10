
#pragma once

#include <string>
#include <unordered_map>

#include "./query.h"

enum TokenType {
  // Single character tokens
  SEMI_COLON,
  LEFT_PAREN,
  RIGHT_PAREN,
  COMMA, DOT, MINUS, PLUS, STAR, SLASH, EQUAL,

  // one or two character tokens
  NOT_EQUAL, GREATER, GREATER_EQUAL, LESS, LESS_EQUAL,

  // literals
  IDENTIFIER, STRING, NUMBER,

  // keywords
  SELECT, AS, FROM, WHERE, AND, OR, IS, NOT, NULL_TOKEN, JOIN,
  ON, CREATE, TABLE, INSERT, INTO, VALUES, DELETE, UPDATE, SET,

  // error keyword
  ERROR_TOKEN
};

struct Token {
public:
  TokenType tokenType;
  int line = 0;
  std::string lexeme;
  int digit;

  bool operator==(const Token& other) const {
    return tokenType == other.tokenType && line == other.line && lexeme == other.lexeme && digit == other.digit;
  }
};

class Lexer {
private:
  std::string input;
  int idx = 0;

  void skipWhiteSpace();
  std::tuple<Token, int> scanToken();
public:
  int line = 1;
  Lexer(std::string input) : input{ input }, idx(0) {};
  Lexer(std::string input, int idx) : input(input), idx(idx) {};

  Token nextToken();
  bool matchToken(TokenType type);
  Token peekToken();
  bool isEOF();
};

struct ErrorMessage {
  std::string message;
  int line;
};

class Parser {
private:
  Lexer lexer;
  std::vector<ErrorMessage> errors;
  void addError(std::string message);
public:
  Parser(std::string input) : lexer(input) {};
  Query parseQuery();
  std::vector<Tuple> parseInsert();
  void parseTable(Query& query);
  std::unique_ptr<Predicate> parsePredicate();
  std::unique_ptr<Term> parseTerm();
  std::unique_ptr<TableValue> parseValue();

};