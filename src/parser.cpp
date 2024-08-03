#include "parser.h"

#include <cstring>
#include <cctype>
#include <unordered_map>

std::unordered_map<std::string, TokenType> keywords = {
  {"SELECT", SELECT},
  {"AS", AS},
  {"FROM", FROM},
  {"WHERE", WHERE},
  {"AND", AND},
  {"OR", OR},
  {"IS", IS},
  {"NOT", NOT},
  {"NULL", NULL_TOKEN},
  {"JOIN", JOIN},
  {"ON", ON}
};


void Lexer::skipWhiteSpace()
{
  while (idx < input.size())
  {
    if (input[idx] == ' ' || input[idx] == '\t' || input[idx] == '\n' || input[idx] == '\r') {
      if (input[idx] == '\n') line++;
      idx++;
    }
    else {
      break;
    }
  }
}

std::tuple<Token, int> Lexer::scanToken()
{
  skipWhiteSpace();
  int start = this->idx;
  Token token;
  switch (this->input[start]) {
  // Single character tokens
  case ';':
    token = { SEMI_COLON, line, "" };
    break;
  case '(':
    token = { LEFT_PAREN, line, "" };
    break;
  case ')':
    token = { RIGHT_PAREN, line, "" };
    break;
  case ',':
    token = { COMMA, line, "" };
    break;
  case '.':
    token = { DOT, line, "" };
    break;
  case '-':
    token = { MINUS, line, "" };
    break;
  case '+':
    token = { PLUS, line, "" };
    break;
  case '*':
    token = { STAR, line, "" };
    break;
  case '/':
    token = { SLASH, line, "" };
    break;
  case '=':
      token = { EQUAL, line, "" };
      break;
  case '>':
    if (start + 1 < input.size() && input[start + 1] == '>') {
      token = { GREATER_EQUAL, line, "" };
      start++;
    }
    else {
      token = { GREATER, line, "" };
    }
    break;
  case '<':
    if (start + 1 < input.size() && input[start + 1] == '=') {
      token = { LESS_EQUAL, line, "" };
      start++;
    }
    else {
      token = { LESS, line, "" };
    }
    break;
  case '"':
  {
    start++;
    int end = start;
    while (end < input.size() && input[end] != '"') {
      end++;
    }
    token = { STRING, line, input.substr(start, end - start) };
    start = end;
    break;
  }

  default:
    if (isdigit(input[start])) {
      int digit = 0;
      while (start < input.size() && isdigit(input[start])) {
        digit = digit * 10 + (input[start] - '0');
        start++;
      }
      start--;
    }
    else if (isalpha(input[start])) {
      std::string lexeme = "";
      while (start < input.size() && isalnum(input[start])) {
        lexeme += input[start];
        start++;
      }
      start--;
      if (keywords.find(lexeme) != keywords.end()) {
        token = { keywords[lexeme], line, lexeme };
      }
      else {
        token = { IDENTIFIER, line, lexeme };
      }
    }
    else {
      token = { ERROR, line, "" };
      // error.
    }
  }


  start++;
  return { token, start };
}

Token Lexer::nextToken()
{
  return Token();
}

Token Lexer::matchToken(TokenType type)
{
  return Token();
}

Token Lexer::peekToken()
{
  return Token();
}

bool Lexer::isEOF()
{
  return idx <= input.size();
}
