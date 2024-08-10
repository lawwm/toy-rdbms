
#include "parser.h"
#include "query.h"

bool Field::operator==(const TableValue* other) const {
  if (auto otherConstant = dynamic_cast<const Field*>(other)) {
    return otherConstant->fieldName == fieldName && otherConstant->table == table;
  }
  else {
    return false;
  }
}

Constant Field::getConstant(Tuple& tuple, Schema& schema) {
  for (int i = 0; i < schema.fieldList.size(); i++) {
    if (schema.fieldList[i] == fieldName) {
      return tuple.fields[i]->getConstant();
    }
  }
  // to do, add empty constant...
}

Constant Constant::getConstant(Tuple& tuple, Schema& schema) {
  return *this;
}

bool Constant::operator==(const TableValue* other) const {
  if (auto otherConstant = dynamic_cast<const Constant*>(other)) {
    return otherConstant->constantType == constantType && otherConstant->num == num && otherConstant->str == str;
  }
  else {
    return false;
  }
}

std::unique_ptr<WriteField> ReadVarCharField::get(Token token) {
  if (token.tokenType != TokenType::STRING) {
    return nullptr;
  }
  else {
    return std::make_unique<VarCharField>(token.lexeme);
  }
}

std::unique_ptr<WriteField> ReadFixedCharField::get(Token token) {
  if (token.tokenType != TokenType::STRING) {
    return nullptr;
  }
  else {
    return std::make_unique<FixedCharField>(length, token.lexeme);
  }
}

std::unique_ptr<WriteField> ReadIntField::get(Token token) {
  if (token.tokenType != TokenType::NUMBER) {
    return nullptr;
  }
  else {
    return std::make_unique<IntField>(token.digit);
  }
}

Tuple Schema::createTuple(std::vector<Token>& tokens) {
  std::vector<std::unique_ptr<WriteField>> writeFields;
  for (int i = 0; i < tokens.size(); i++) {
    writeFields.push_back(fieldMap[fieldList[i]]->get(tokens[i]));
  }
  return Tuple(std::move(writeFields));
}