
#include <cstring>
#include <cctype>
#include <unordered_map>
#include <memory>

#include "./parser.h"
#include "./query.h"

std::unordered_map<std::string, TokenType> keywords = {
  // capitalised keywords
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
  {"ON", ON},
  {"CREATE", CREATE},
  {"TABLE", TABLE},
  {"INSERT", INSERT},
  {"INTO", INTO},
  {"VALUES", VALUES},
  {"DELETE", DELETE},
  {"UPDATE", UPDATE},
  {"SET", SET},

  // do the uncapitalised keywords

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
    if (start + 1 < input.size() && input[start + 1] == '=') {
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
  case '!':
    if (start + 1 < input.size() && input[start + 1] == '=') {
      token = { NOT_EQUAL, line, "" };
      start++;
    }
    else {
      token = { NOT, line, "" };
    }
    break;
  case '"':
  case '\'':
  {
    char startingQuote = input[start];
    start++;
    int end = start;
    while (end < input.size() && input[end] != startingQuote) {
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
      token = { NUMBER, line, "", digit };
    }
    else if (isalpha(input[start])) {
      std::string lexeme = "";
      while (start < input.size() && (isalnum(input[start]) || input[start] == '_')) {
        lexeme += input[start];
        start++;
      }
      start--;
      if (keywords.find(lexeme) != keywords.end()) {
        token = { keywords[lexeme], line, "" };
      }
      else {
        token = { IDENTIFIER, line, lexeme };
      }
    }
    else {
      // error.
      token = { ERROR_TOKEN, line, "" };
    }
    break;
  }


  start++;
  return { token, start };
}

Token Lexer::nextToken()
{
  auto [token, idx] = this->scanToken();
  this->idx = idx;
  return token;
}

bool Lexer::matchToken(TokenType type)
{
  return peekToken().tokenType == type;
}

Token Lexer::peekToken()
{
  auto [token, idx] = this->scanToken();
  return token;
}

bool Lexer::isEOF()
{
  return idx >= input.size();
}


void Parser::addError(std::string message)
{
  errors.push_back(ErrorMessage{ message, this->lexer.line });
}

std::variant<Query, Insert, Schema> Parser::parseStatement()
{
  if (lexer.matchToken(SELECT)) {
    return this->parseQuery();
  }
  else if (lexer.matchToken(CREATE)) {
    return this->parseCreate();
  }
  else if (lexer.matchToken(INSERT)) {
    return this->parseInsert();
  }
  else {
    throw "no proper statements given here, to refactor in the future";
  }
}

Query Parser::parseQuery()
{
  Query query;
  if (!lexer.matchToken(SELECT)) {
    this->addError("Expected SELECT keyword");
  }

  do {
    lexer.nextToken();
    auto selectField = this->parseValue();
    query.addField(std::move(selectField));
  } while (lexer.matchToken(COMMA));

  if (!lexer.matchToken(FROM)) {
    this->addError("Expected FROM keyword");
  }
  lexer.nextToken();
  this->parseTable(query);

  // add where predicate
  if (lexer.matchToken(WHERE)) {
    lexer.nextToken();
    auto pred = this->parsePredicate();
    query.addPredicate(std::move(pred));
  }

  if (!lexer.matchToken(SEMI_COLON)) {
    this->addError("Expected semicolon");
  }
  lexer.nextToken();
  if (!lexer.isEOF()) {
    this->addError("Unexpected token");
  }
  return query;
}

Insert Parser::parseInsert()
{
  Insert insertStmt;

  // parse table name
  if (!lexer.matchToken(INSERT)) {
    this->addError("Expected INSERT keyword");
  }
  lexer.nextToken();
  if (!lexer.matchToken(INTO)) {
    this->addError("Expected INTO keyword");
  }
  lexer.nextToken();

  if (!lexer.matchToken(IDENTIFIER)) {
    this->addError("Expected table name");
  }

  // store table value
  auto tableToken = lexer.nextToken();
  insertStmt.table = tableToken.lexeme;

  // parse specific columns
  if (lexer.matchToken(LEFT_PAREN)) {
    lexer.nextToken();
    while (lexer.matchToken(IDENTIFIER)) {
      insertStmt.fields.push_back(lexer.nextToken().lexeme);
      if (lexer.matchToken(COMMA)) {
        lexer.nextToken();
      }
    }
    if (!lexer.matchToken(RIGHT_PAREN)) {
      this->addError("Expected right parenthesis");
    }
    lexer.nextToken();
  }

  if (!lexer.matchToken(VALUES)) {
    this->addError("Expected VALUES keyword");
  }
  lexer.nextToken();

  // parse tuples
  do {
    if (!lexer.matchToken(LEFT_PAREN)) {
      this->addError("Expected left parenthesis");
    }
    lexer.nextToken();

    std::vector<Token> values;
    do {
      values.push_back(lexer.nextToken());
      if (lexer.matchToken(COMMA)) {
        lexer.nextToken();
      }
      else {
        break;
      }
    } while (true);

    insertStmt.values.push_back(std::move(values));

    if (!lexer.matchToken(RIGHT_PAREN)) {
      this->addError("Expected right parenthesis");
    }
    lexer.nextToken();


    if (lexer.matchToken(COMMA)) {
      lexer.nextToken();
      continue;
    }
    else {
      break;
    }
  } while (true);

  if (!lexer.matchToken(SEMI_COLON)) {
    this->addError("Expected semicolon");
  }
  lexer.nextToken();

  return insertStmt;
};


Schema Parser::parseCreate() {

  if (!lexer.matchToken(CREATE)) {
    this->addError("Expected CREATE keyword");
  }
  lexer.nextToken();
  if (!lexer.matchToken(TABLE)) {
    this->addError("Expected TABLE keyword");
  }
  lexer.nextToken();

  auto tableName = lexer.nextToken();

  if (!lexer.matchToken(LEFT_PAREN)) {
    this->addError("Expected left parenthesis");
  }
  lexer.nextToken();
  Schema schema;

  do {
    // get field name
    auto fieldName = lexer.nextToken();

    // store type
    auto fieldType = this->parseType();
    if (fieldType != nullptr) {
      schema.addField(tableName.lexeme, fieldName.lexeme, std::move(fieldType));
    }

    if (lexer.matchToken(COMMA)) {
      lexer.nextToken();
      continue;
    }
    else {
      break;
    }

  } while (true);

  if (!lexer.matchToken(RIGHT_PAREN)) {
    this->addError("Expected right parenthesis");
  }
  lexer.nextToken();
  if (!lexer.matchToken(SEMI_COLON)) {
    this->addError("Expected semicolon");
  }
  lexer.nextToken();

  return schema;
}

void Parser::parseTable(Query& query)
{
  if (lexer.matchToken(IDENTIFIER)) {
    Token table = lexer.nextToken();
    query.addJoinTable(table.lexeme);
  }
  else if (lexer.matchToken(LEFT_PAREN)) {
    lexer.nextToken();
    this->parseTable(query);
    if (!lexer.matchToken(RIGHT_PAREN)) {
      this->addError("Expected right parenthesis");
    }
    lexer.nextToken();
  }
  else {
    this->addError("Expected table name");
  }

  while (lexer.matchToken(JOIN)) {
    lexer.nextToken();
    auto nextToken = lexer.nextToken();
    if (nextToken.tokenType == LEFT_PAREN) {
      this->parseTable(query);
      lexer.nextToken();
    }
    else {
      query.addJoinTable(nextToken.lexeme);
      lexer.nextToken(); // ON
    }

    auto pred = this->parsePredicate();
    query.addPredicate(std::move(pred));
  }
}

// pred ::= [term | (pred)] [AND | OR pred]
std::unique_ptr<Predicate> Parser::parsePredicate()
{
  std::unique_ptr<Predicate> lhs;
  if (lexer.matchToken(LEFT_PAREN)) {
    lexer.nextToken();
    lhs = this->parsePredicate();
    auto tkn = lexer.peekToken();
    if (!lexer.matchToken(RIGHT_PAREN)) {
      this->addError("Expected a closing brace");
    }
    lexer.nextToken();
  }
  else {
    lhs = std::make_unique<Predicate>(this->parseTerm());
  }

  while (lexer.matchToken(AND) || lexer.matchToken(OR)) {
    auto op = lexer.nextToken();
    TokenType tokenType = op.tokenType;
    PredicateOperand po;
    if (tokenType == AND) {
      po = PredicateOperand::AND;
    }
    else if (tokenType == OR) {
      po = PredicateOperand::OR;
    }
    else {
      this->addError("Expected AND or OR");
      break; //error
    }
    std::unique_ptr<Predicate> rhs;
    if (lexer.matchToken(LEFT_PAREN)) {
      lexer.nextToken();
      rhs = this->parsePredicate();
      if (!lexer.matchToken(RIGHT_PAREN)) {
        this->addError("Expected a closing brace");
      }
      lexer.nextToken();
    }
    else {
      rhs = std::make_unique<Predicate>(this->parseTerm());
    }
    lhs = std::make_unique<Predicate>(po, std::move(lhs), std::move(rhs));
  }

  return lhs;
}

std::unique_ptr<Term> Parser::parseTerm()
{
  auto lhs = parseValue();
  auto op = lexer.nextToken();
  auto rhs = parseValue();

  if (lhs && rhs) {
    if (op.tokenType == EQUAL || op.tokenType == NOT_EQUAL || op.tokenType == GREATER || op.tokenType == GREATER_EQUAL || op.tokenType == LESS || op.tokenType == LESS_EQUAL) {
      TermOperand to;
      switch (op.tokenType) {
      case EQUAL:
        to = TermOperand::EQUAL;
        break;
      case NOT_EQUAL:
        to = TermOperand::NOT_EQUAL;
        break;
      case GREATER:
        to = TermOperand::GREATER;
        break;
      case GREATER_EQUAL:
        to = TermOperand::GREATER_EQUAL;
        break;
      case LESS:
        to = TermOperand::LESS;
        break;
      case LESS_EQUAL:
        to = TermOperand::LESS_EQUAL;
        break;
      }
      return std::make_unique<Term>(to, std::move(lhs), std::move(rhs));
    }
    else {
      // display error here.
      errors.push_back(ErrorMessage{ "Expected a comparison operator" });
    }
  }

  return nullptr;
}

std::unique_ptr<TableValue> Parser::parseValue()
{
  if (lexer.matchToken(STRING)) {
    Token token = lexer.nextToken();
    std::unique_ptr<TableValue> tv = std::make_unique<Constant>(token.lexeme);
    return tv;
  }
  else if (lexer.matchToken(NUMBER)) {
    Token token = lexer.nextToken();
    std::unique_ptr<TableValue> tv = std::make_unique<Constant>(token.digit);
    return tv;
  }
  else if (lexer.matchToken(IDENTIFIER)) {
    Token field = lexer.nextToken();
    if (lexer.matchToken(DOT)) {
      lexer.nextToken();
      auto fieldName = lexer.nextToken();
      return std::make_unique<Field>(field.lexeme, fieldName.lexeme);
    }
    else {
      return std::make_unique<Field>(field.lexeme, field.lexeme);
    }
  }


  // display error here.
  errors.push_back(ErrorMessage{ "Expected a value" });
  return nullptr;
}

std::unique_ptr<ReadField> Parser::parseType() {
  auto fieldType = lexer.nextToken();
  if (fieldType.tokenType != IDENTIFIER) {
    this->addError("Expected field type");
  }

  if (fieldType.lexeme == "INT") {
    return std::make_unique<ReadIntField>();
  }
  else if (fieldType.lexeme == "VARCHAR" || fieldType.lexeme == "CHAR") {

    u32 defaultFieldSize = 100;

    if (lexer.matchToken(LEFT_PAREN)) {
      lexer.nextToken();
      auto fieldSize = lexer.nextToken();
      if (fieldSize.tokenType != NUMBER) {
        this->addError("Expected field size");
      }
      if (!lexer.matchToken(RIGHT_PAREN)) {
        this->addError("Expected right parenthesis");
      }
      lexer.nextToken();

      defaultFieldSize = fieldSize.digit;
    }

    if (fieldType.lexeme == "VARCHAR") {
      return std::make_unique<ReadVarCharField>();
    }
    else {
      return std::make_unique<ReadFixedCharField>(defaultFieldSize);
    }
  }
  else {
    this->addError("Invalid field type");
    return nullptr;
  }
}