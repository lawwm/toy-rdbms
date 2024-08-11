
#include "parser.h"
#include "query.h"
#include "buffer.h"
#include "scan.h"

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

std::unordered_map<std::string, Schema> getSchemaFromTableName(std::vector<std::string> tblNames, std::shared_ptr<ResourceManager> rm)
{
  if (tblNames.size() == 0) {
    return {};
  }
  std::vector<std::unique_ptr<Predicate>> terms;

  std::unique_ptr<Predicate> predicate = std::make_unique<Predicate>(std::make_unique<Term>(TermOperand::EQUAL, std::make_unique<Field>(SCHEMA_TABLE, TABLE_NAME), std::make_unique<Constant>(tblNames[0])));
  for (u32 i = 1; i < tblNames.size(); ++i) {
    predicate = std::make_unique<Predicate>(PredicateOperand::OR,
      std::move(predicate),
      std::make_unique<Predicate>(std::make_unique<Term>(TermOperand::EQUAL, std::make_unique<Field>(SCHEMA_TABLE, TABLE_NAME), std::make_unique<Constant>(tblNames[i]))));
  }

  std::unique_ptr<Scan> tableScan = std::make_unique<TableScan>(SCHEMA_TABLE, rm, schemaTable);
  std::unique_ptr<Scan> selectScan = std::make_unique<SelectScan>(std::move(tableScan), std::move(predicate), schemaTable);

  std::vector<Tuple> tuples;
  selectScan->getFirst();
  while (selectScan->next()) {
    auto tuple = selectScan->get();
    tuples.push_back(std::move(tuple));
  }

  std::sort(begin(tuples), end(tuples), [](auto& lhs, auto& rhs) {
    auto lhsOrder = lhs.fields[3]->getConstant().num;
    auto rhsOrder = rhs.fields[3]->getConstant().num;
    return lhsOrder < rhsOrder;
    });

  std::unordered_map<std::string, Schema> res;

  for (auto& tuple : tuples) {
    std::string tableName = tuple.fields[0]->getConstant().str;
    std::string fieldName = tuple.fields[1]->getConstant().str;
    std::string fieldType = tuple.fields[2]->getConstant().str;

    if (res.find(tableName) == res.end()) {
      res.emplace(tableName, Schema(tableName));
    }

    Parser parser(fieldType);
    auto writePtr = parser.parseType();

    res.at(tableName).addField(fieldName, std::move(writePtr));
  }

  return res;
}
