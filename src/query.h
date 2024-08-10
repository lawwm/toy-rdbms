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

private:
  ConstantType constantType;
  int num;
  std::string str;
};


class Field : public TableValue {
public:
  virtual ~Field() override {}
  Field(std::string table, std::string fieldName) : fieldName{ fieldName }, table{ table } {}
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
};


// first 4 bytes is string length, then the string
class VarCharField : public WriteField {
  std::string value;
public:
  virtual ~VarCharField() override = default;
  VarCharField(std::string value) : value{ value } {}

  virtual u32 getLength() override {
    return value.size() + sizeof(u32);
  }
  virtual void write(char* buffer, u32 offset) override {
    u32 length = value.size();
    std::memcpy(buffer + offset, &length, sizeof(u32));
    std::memcpy(buffer + offset + sizeof(u32), value.c_str(), value.size());
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
    u32 length;
    std::memcpy(&length, buffer + offset, sizeof(u32));
    std::string value(buffer + offset + sizeof(u32), length);
    return std::make_unique<VarCharField>(value);
  }

  virtual std::unique_ptr<WriteField> get(Token token) override;
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

struct Schema {
  std::string filename;
  std::vector<std::string> fieldList;
  std::unordered_map<std::string, std::unique_ptr<ReadField>> fieldMap;

  Schema(std::string filename) : filename{ filename } {};
  Schema(const Schema& other) : filename{ other.filename }, fieldList{ other.fieldList } {
    for (auto& field : other.fieldMap) {
      fieldMap[field.first] = field.second->clone();
    }
  }
  Schema& operator==(const Schema& other) {
    if (this == &other) {
      return *this;
    }
    filename = other.filename;
    fieldList = other.fieldList;
    for (auto& field : other.fieldMap) {
      fieldMap[field.first] = field.second->clone();
    }
    return *this;
  }

  void addField(std::string fieldName, std::unique_ptr<ReadField> field) {
    fieldMap[fieldName] = std::move(field);
    fieldList.push_back(fieldName);
  }

  Tuple createTuple(std::vector<Token>& token);
};

// schema table
static Schema schemaTable = []() {
  Schema schemaTable("schema");
  schemaTable.addField("filename", std::make_unique<ReadVarCharField>());
  schemaTable.addField("field_name", std::make_unique<ReadVarCharField>());
  schemaTable.addField("field_type", std::make_unique<ReadVarCharField>());
  return schemaTable;
  }();

