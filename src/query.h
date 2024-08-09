#pragma once

#include <vector>
#include <string>
#include <memory>
#include "common.h"

class TableValue {
public:
  virtual ~TableValue() = default;
  virtual bool operator==(const TableValue* other) const = 0;
};

class Field : public TableValue {
public:
  virtual ~Field() override {}
  Field(std::string table, std::string fieldName) : table{ table }, fieldName{ fieldName } {}
  bool operator==(const TableValue* other) const override {
    if (auto otherConstant = dynamic_cast<const Field*>(other)) {
      return otherConstant->fieldName == fieldName && otherConstant->table == table;
    }
    else {
      return false;
    }
  }
private:
  std::string fieldName;
  std::string table;
};

enum class ConstantType {
  STRING, NUMBER
};

class Constant : public TableValue {
public:
  virtual ~Constant() override {}
  Constant(int num) : type{ ConstantType::NUMBER }, num{ num } {}
  Constant(std::string str) : type{ ConstantType::STRING }, str{ str } {}

  bool operator==(const TableValue* other) const override {
    if (auto otherConstant = dynamic_cast<const Constant*>(other)) {
      return otherConstant->type == type && otherConstant->num == num && otherConstant->str == str;
    }
    else {
      return false;
    }
  }
private:
  ConstantType type;
  int num;
  std::string str;
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


class WriteField {
public:
  virtual ~WriteField() = default;
  virtual u32 getLength() = 0;
  virtual void write(char* buffer, u32 offset) = 0;
};

class ReadField {
public:
  virtual ~ReadField() = default;
  virtual WriteField* get(const char* buffer, u32 offset) = 0;
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
};

class ReadVarCharField : public ReadField {
public:
  ReadVarCharField() {}
  virtual ~ReadVarCharField() override = default;

  virtual WriteField* get(const char* buffer, u32 offset) override {
    u32 length;
    std::memcpy(&length, buffer + offset, sizeof(u32));
    std::string value(buffer + offset + sizeof(u32), length);
    return new VarCharField(value);
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
};

class ReadFixedCharField : public ReadField {
private:
  int length;
public:
  ReadFixedCharField(int length) : length{ length } {}
  virtual ~ReadFixedCharField() override = default;

  virtual WriteField* get(const char* buffer, u32 offset) override {
    std::string value(buffer + offset + sizeof(u32), length);
    return new FixedCharField(length, value);
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
};

class ReadIntField : public ReadField {
public:
  ReadIntField() {}
  virtual ~ReadIntField() override = default;

  virtual WriteField* get(const char* buffer, u32 offset) override {
    i32 value;
    std::memcpy(&value, buffer + offset, sizeof(i32));
    return new IntField(value);
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
};

struct Schema {
  std::string filename;
  std::vector<ReadField*> fields;

  Schema(std::string filename, std::vector<ReadField*> fields) : filename{ filename }, fields{ fields } {};
};