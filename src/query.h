#pragma once

#include <vector>
#include <string>
#include <memory>
#include <unordered_map>
#include "common.h"

struct Tuple;
struct Schema;
class Constant;
struct Token;

class TableValue {
public:
  virtual ~TableValue() = default;
  virtual bool operator==(const TableValue* other) const = 0;
  virtual Constant getConstant(Tuple& tuple, Schema& schema) = 0;
};

enum class ConstantType {
  STRING, NUMBER
};

class Constant : public TableValue {
public:
  virtual ~Constant() override {}
  Constant(int num) : constantType{ ConstantType::NUMBER }, num{ num } {}
  Constant(std::string str) : constantType{ ConstantType::STRING }, str{ str } {}

  bool operator==(const TableValue* other) const override;
  virtual Constant getConstant(Tuple& tuple, Schema& schema) override;

  bool operator==(const Constant& other) const {
    if (constantType != other.constantType) {
      return false;
    }
    if (constantType == ConstantType::NUMBER) {
      return num == other.num;
    }
    return str == other.str;
  }

  bool operator!=(const Constant& other) const {
    return !(*this == other);
  }

  bool operator>=(const Constant& other) const {
    if (constantType != other.constantType) {
      return false;
    }
    if (constantType == ConstantType::NUMBER) {
      return num >= other.num;
    }
    return str >= other.str;
  }

  bool operator>(const Constant& other) const {
    if (constantType != other.constantType) {
      return false;
    }
    if (constantType == ConstantType::NUMBER) {
      return num > other.num;
    }
    return str > other.str;
  }

  bool operator<=(const Constant& other) const {
    if (constantType != other.constantType) {
      return false;
    }
    if (constantType == ConstantType::NUMBER) {
      return num <= other.num;
    }
    return str <= other.str;
  }

  bool operator<(const Constant& other) const {
    if (constantType != other.constantType) {
      return false;
    }
    if (constantType == ConstantType::NUMBER) {
      return num < other.num;
    }
    return str < other.str;
  }

  ConstantType constantType;
  int num;
  std::string str;
};


class Field : public TableValue {
public:
  virtual ~Field() override {}
  Field(std::string table, std::string fieldName) : fieldName{ fieldName }, table{ table } {}
  Field(std::string fieldName) : fieldName{ fieldName }, table{ fieldName } {}
  bool operator==(const TableValue* other) const override;
  virtual Constant getConstant(Tuple& tuple, Schema& schema) override;
private:
  std::string fieldName;
  std::string table;
};



class WriteField {
public:
  virtual ~WriteField() = default;
  virtual u32 getLength() = 0;
  virtual void write(char* buffer, u32 offset) = 0;
  virtual Constant getConstant() = 0;
};

class ReadField {
public:
  virtual ~ReadField() = default;
  virtual std::unique_ptr<ReadField> clone() = 0;
  virtual std::unique_ptr<WriteField> get(const char* buffer, u32 offset) = 0;
  virtual std::unique_ptr<WriteField> get(Token token) = 0;
  virtual std::unique_ptr<WriteField> get(Constant token) = 0;
  virtual std::string serializeType() = 0;
};


// first 2 bytes is string length, then physical length then the string
class VarCharField : public WriteField {
  u16 physicalSize;
  std::string value;
public:
  virtual ~VarCharField() override = default;
  VarCharField(std::string value) :physicalSize{ (u16)value.size() }, value{ value } {}
  VarCharField(std::string value, u16 physicalSize) : physicalSize{ physicalSize }, value{ value } {}

  virtual u32 getLength() override {
    return physicalSize + sizeof(u32);
  }

  virtual void write(char* buffer, u32 offset) override {
    u16 length = (u16)value.size();
    std::memcpy(buffer + offset, &length, sizeof(u16));
    std::memcpy(buffer + offset + sizeof(u16), &physicalSize, sizeof(u16));
    std::memcpy(buffer + offset + sizeof(u16) + sizeof(u16), value.c_str(), length);
  }

  virtual Constant getConstant() override {
    return Constant(value);
  }
};

class ReadVarCharField : public ReadField {
public:
  ReadVarCharField() {}
  virtual ~ReadVarCharField() = default;

  virtual std::unique_ptr<ReadField> clone() override {
    return std::make_unique<ReadVarCharField>();
  }

  virtual std::unique_ptr<WriteField> get(const char* buffer, u32 offset) override {
    u16 length, physicalSize;
    std::memcpy(&length, buffer + offset, sizeof(u16));
    std::memcpy(&physicalSize, buffer + offset + sizeof(u16), sizeof(u16));
    std::string value(buffer + offset + sizeof(u16) + sizeof(u16), length);
    return std::make_unique<VarCharField>(value);
  }

  virtual std::unique_ptr<WriteField> get(Token token) override;

  std::unique_ptr<WriteField> get(Constant constant) override;

  virtual std::string serializeType() override {
    return "VARCHAR";
  }
};

class FixedCharField : public WriteField {
private:
  int length;
  std::string value;
public:
  FixedCharField(int length, std::string value) : length{ length }, value{ value } {}
  virtual ~FixedCharField() override = default;

  virtual u32 getLength() override {
    return length;
  }
  virtual void write(char* buffer, u32 offset) override {
    std::memcpy(buffer + offset, value.c_str(), value.size());
  }
  virtual Constant getConstant() override {
    return Constant(value);
  }


};

class ReadFixedCharField : public ReadField {
private:
  int length;
public:
  ReadFixedCharField(int length) : length{ length } {}
  virtual ~ReadFixedCharField() override = default;

  virtual std::unique_ptr<ReadField> clone() override {
    return std::make_unique<ReadFixedCharField>(length);
  }

  virtual std::unique_ptr<WriteField> get(const char* buffer, u32 offset) override {
    std::string value(buffer + offset);
    return std::make_unique<FixedCharField>(length, value);
  }

  virtual std::unique_ptr<WriteField> get(Token token) override;

  std::unique_ptr<WriteField> get(Constant constant) override;


  virtual std::string serializeType() override {
    return "CHAR(" + std::to_string(length) + ")";
  }
};

class IntField : public WriteField {
private:
  i32 value;
public:
  IntField(i32 value) : value{ value } {}
  virtual ~IntField() override = default;

  virtual u32 getLength() override {
    return sizeof(i32);
  }
  virtual void write(char* buffer, u32 offset) override {
    std::memcpy(buffer + offset, &value, sizeof(i32));
  }
  virtual Constant getConstant() override {
    return Constant(value);
  }
};

class ReadIntField : public ReadField {
public:
  ReadIntField() {}
  virtual ~ReadIntField() override = default;

  virtual std::unique_ptr<ReadField> clone() override {
    return std::make_unique<ReadIntField>();
  }

  virtual std::unique_ptr<WriteField> get(const char* buffer, u32 offset) override {
    i32 value;
    std::memcpy(&value, buffer + offset, sizeof(i32));
    return std::make_unique<IntField>(value);
  }

  virtual std::unique_ptr<WriteField> get(Token token) override;

  std::unique_ptr<WriteField> get(Constant constant) override;


  virtual std::string serializeType() override {
    return "INT";
  }
};

struct Tuple {
  std::vector<std::unique_ptr<WriteField>> fields;
  u32 recordSize;
  Tuple(std::vector<std::unique_ptr<WriteField>> inputFields) : fields{ std::move(inputFields) } {
    recordSize = 0;
    for (auto& field : this->fields) {
      recordSize += field->getLength();
    }
  }

  u32 set(u32 idx, std::unique_ptr<WriteField> field) {
    if (idx >= fields.size()) {
      return 0;
    }
    fields[idx] = std::move(field);
    recordSize = 0;
    for (auto& field : this->fields) {
      recordSize += field->getLength();
    }
    return recordSize;
  }
};

enum class TermOperand {
  EQUAL, NOT_EQUAL, GREATER, GREATER_EQUAL, LESS, LESS_EQUAL,
};


class Term {
public:
  // move constructor
  Term(TermOperand op, std::unique_ptr<TableValue>&& lhs, std::unique_ptr<TableValue>&& rhs) : op{ op }, lhs{ std::move(lhs) }, rhs{ std::move(rhs) } {}
  bool operator==(const Term& other) const {
    const TableValue* lhsVal = this->lhs.get();
    const TableValue* rhsVal = this->rhs.get();
    if (op == other.op && lhsVal == nullptr && rhsVal == nullptr && other.lhs == nullptr && other.rhs == nullptr) {
      return true;
    }
    if (lhsVal != nullptr && rhsVal != nullptr) {
      return op == other.op && *lhsVal == other.lhs.get() && *rhsVal == other.rhs.get();
    }

    return false;
  }

  bool evaluate(Tuple& tuple, Schema& schema) {
    Constant lhsConstant = lhs->getConstant(tuple, schema);
    Constant rhsConstant = rhs->getConstant(tuple, schema);
    switch (op) {
    case TermOperand::EQUAL:
      return lhsConstant == rhsConstant;
    case TermOperand::NOT_EQUAL:
      return lhsConstant != rhsConstant;
    case TermOperand::GREATER:
      return lhsConstant > rhsConstant;
    case TermOperand::GREATER_EQUAL:
      return lhsConstant >= rhsConstant;
    case TermOperand::LESS:
      return lhsConstant < rhsConstant;
    case TermOperand::LESS_EQUAL:
      return lhsConstant <= rhsConstant;
    }
  }

private:
  TermOperand op;
  std::unique_ptr<TableValue> lhs;
  std::unique_ptr<TableValue> rhs;
};

enum class PredicateOperand {
  SINGLE, AND, OR
};

class Predicate {

public:
  Predicate(PredicateOperand op, std::unique_ptr<Predicate>&& lhs, std::unique_ptr<Predicate>&& rhs) : op{ op }, lhs{ std::move(lhs) }, rhs{ std::move(rhs) } {}
  Predicate(std::unique_ptr<Term>&& lhs) : op{ PredicateOperand::SINGLE }, term{ std::move(lhs) } {}
  bool operator==(const Predicate& other) const {
    if (this->op != other.op) {
      return false;
    }

    if (op == PredicateOperand::SINGLE) {
      if (term != nullptr && other.term != nullptr) {
        return *term == *(other.term);
      }
      return false;
    }
    else {
      if (lhs != nullptr && rhs != nullptr && other.lhs != nullptr && other.rhs != nullptr) {
        return *lhs == *(other.lhs) && *rhs == *(other.rhs);
      }
      return false;
    }
  }

  bool evaluate(Tuple& tuple, Schema& schema) {
    if (op == PredicateOperand::SINGLE) {
      return term->evaluate(tuple, schema);
    }
    else if (op == PredicateOperand::AND) {
      return lhs->evaluate(tuple, schema) && rhs->evaluate(tuple, schema);
    }
    else {
      return lhs->evaluate(tuple, schema) || rhs->evaluate(tuple, schema);
    }
  }

private:
  PredicateOperand op;
  std::unique_ptr<Term> term;
  std::unique_ptr<Predicate> lhs;
  std::unique_ptr<Predicate> rhs;
};

class Query {
public:
  Query() = default;
  void addField(std::unique_ptr<TableValue> field) {
    selectFields.push_back(std::move(field));
  }
  void addJoinTable(std::string table) {
    joinTable.push_back(table);
  }
  void addPredicate(std::unique_ptr<Predicate> pred) {
    predicate.push_back(std::move(pred));
  }

  std::vector<std::unique_ptr<TableValue>> selectFields;
  std::vector<std::string> joinTable;
  std::vector<std::unique_ptr<Predicate>> predicate;
};

struct Insert {
  std::string table;
  std::vector<std::string > fields;
  std::vector<std::vector<Token>> values;
};


struct UpdateStmt {
  std::string table;
  std::vector<std::unique_ptr<Predicate>> predicate;
  std::unordered_map<std::unique_ptr<Field>, std::unique_ptr<TableValue>> setFields;
};

struct DeleteStmt {
  std::string table;
  std::vector<std::unique_ptr<Predicate>> predicate;
};

const std::string TABLE_NAME = "table_name";
const std::string FIELD_NAME = "field_name";
const std::string FIELD_TYPE = "field_type";
const std::string ORDER = "order";
const std::string SCHEMA_TABLE = "schema";

struct Schema {
  std::vector<std::string> tableList;
  std::vector<std::string> fieldList;
  std::unordered_map<std::string, std::unique_ptr<ReadField>> fieldMap;

  Schema() {};
  Schema(const Schema& other) : tableList{ other.tableList }, fieldList{ other.fieldList } {
    for (auto& field : other.fieldMap) {
      fieldMap[field.first] = field.second->clone();
    }
  }
  Schema(Schema&& other) : tableList{ std::move(other.tableList) }, fieldList{ std::move(other.fieldList) },
    fieldMap{ std::move(other.fieldMap) } {}

  Schema& operator==(const Schema& other) {
    if (this == &other) {
      return *this;
    }
    tableList = other.tableList;
    fieldList = other.fieldList;
    for (auto& field : other.fieldMap) {
      fieldMap[field.first] = field.second->clone();
    }
    return *this;
  }

  Schema& operator==(Schema&& other) {
    if (this == &other) {
      return *this;
    }
    tableList = std::move(other.tableList);
    fieldList = std::move(other.fieldList);
    fieldMap = std::move(other.fieldMap);
    return *this;
  }

  void addField(std::string tableName, std::string fieldName, std::unique_ptr<ReadField> field) {
    fieldMap[fieldName] = std::move(field);
    tableList.push_back(tableName);
    fieldList.push_back(fieldName);
  }

  Tuple createTuple(std::vector<Token>& token);

  std::vector<Tuple> createSchemaTuple() {
    std::vector<Tuple> tuples;
    for (int i = 0; i < fieldList.size(); ++i) {
      std::vector<std::unique_ptr<WriteField>> writeFields;
      writeFields.push_back(std::make_unique<VarCharField>(tableList[i]));
      writeFields.push_back(std::make_unique<VarCharField>(fieldList[i]));
      writeFields.push_back(std::make_unique<VarCharField>(fieldMap[fieldList[i]]->serializeType()));
      writeFields.push_back(std::make_unique<IntField>(i));
      tuples.push_back(Tuple(std::move(writeFields)));
    }

    return tuples;
  }
};


struct ResourceManager;
std::unordered_map<std::string, Schema> getSchemaFromTableName(std::vector<std::string> tblNames, std::shared_ptr<ResourceManager> rm);

// schema table
static Schema schemaTable = []() {
  Schema schemaTable;
  schemaTable.addField(SCHEMA_TABLE, TABLE_NAME, std::make_unique<ReadVarCharField>());
  schemaTable.addField(SCHEMA_TABLE, FIELD_NAME, std::make_unique<ReadVarCharField>());
  schemaTable.addField(SCHEMA_TABLE, FIELD_TYPE, std::make_unique<ReadVarCharField>());
  schemaTable.addField(SCHEMA_TABLE, ORDER, std::make_unique<ReadIntField>()); // start from zero
  return schemaTable;
  }();

