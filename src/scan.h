#pragma once

#include "query.h"
#include "buffer.h"

class Scan {
public:
  virtual ~Scan() {};
  virtual bool getFirst() = 0;
  virtual bool next() = 0;
  virtual Tuple get() = 0;
  virtual Schema& getSchema() = 0;
};

class UpdateScan {
public:
  virtual ~UpdateScan() {};
  virtual void insert(Tuple& tuple) = 0;
  virtual void update(Tuple& tuple) = 0;
  virtual void deleteTuple() = 0;
};

class TableScan : public Scan {
private:
  PageId currentPageId;
  i32 currentSlot;
  std::shared_ptr<ResourceManager> rm;
  BufferFrame* currBuffer;
  std::string filename;
  Schema schema;

  bool findNextPage();

public:
  TableScan(std::string filename, std::shared_ptr<ResourceManager> rm, Schema schema) :
    currentPageId{ PageId{ filename, 0 } }, currentSlot{ -1 },
    rm{ rm }, currBuffer{ nullptr }, filename{ filename }, schema{ schema } {
  }
  ~TableScan() = default;

  bool getFirst() override;

  bool next() override;

  Tuple get() override;

  Schema& getSchema() override;
};


class SelectScan : public Scan {
private:
  std::unique_ptr<Scan> scan;
  std::unique_ptr<Predicate> predicate;

public:
  SelectScan(std::unique_ptr<Scan> scan, std::unique_ptr<Predicate> predicate) :
    scan{ std::move(scan) }, predicate{ std::move(predicate) } {
  }

  ~SelectScan() = default;

  bool getFirst() override;

  bool next() override;

  Tuple get() override;

  Schema& getSchema() override;
};

class ProductScan : public Scan {
private:
  std::unique_ptr<Scan> leftScan;
  std::unique_ptr<Scan> rightScan;
  Schema schema;
  // this map tells how to get the fields
  // (lhs or rhs, index).
  // lhs or rhs shows whether lhs or rhs contains the field, 0 -> lhs, 1 -> rhs
  // index is the index of the field in the lhs/rhs schema

public:
  ProductScan(std::unique_ptr<Scan> leftScanInput, std::unique_ptr<Scan> rightScanInput) :
    leftScan{ std::move(leftScanInput) }, rightScan{ std::move(rightScanInput) } {
    auto& lhsSchema = leftScan->getSchema();
    auto& rhsSchema = rightScan->getSchema();
    for (u32 i = 0; i < lhsSchema.fieldList.size(); ++i) {
      schema.addField(lhsSchema.tableList[i], lhsSchema.fieldList[i], lhsSchema.fieldMap[lhsSchema.fieldList[i]]->clone());
    }
    for (u32 i = 0; i < rhsSchema.fieldList.size(); ++i) {
      schema.addField(rhsSchema.tableList[i], rhsSchema.fieldList[i], rhsSchema.fieldMap[rhsSchema.fieldList[i]]->clone());
    }
  }

  ~ProductScan() = default;

  bool getFirst() override;

  bool next() override;

  Tuple get() override;

  Schema& getSchema() override;
};

class ProjectScan : public Scan {
private:
  std::unique_ptr<Scan> scan;
  std::vector<u32> mapToInnerSchema;
  Schema schema;
public:
  ProjectScan(std::unique_ptr<Scan> inputScan, std::vector<std::unique_ptr<TableValue>>& fields) :
    scan{ std::move(inputScan) } {
    auto& innerSchema = this->scan->getSchema();
    for (auto& field : fields) {
      bool foundField = false;
      for (u32 i = 0; i < innerSchema.fieldList.size(); ++i) {
        std::unique_ptr<TableValue> fieldOnly = std::make_unique<Field>(innerSchema.tableList[i], innerSchema.fieldList[i]);
        std::unique_ptr<TableValue> tableAndField = std::make_unique<Field>(innerSchema.fieldList[i], innerSchema.fieldList[i]);
        if (*fieldOnly == field.get() || *tableAndField == field.get()) {
          mapToInnerSchema.push_back(i);
          schema.addField(innerSchema.tableList[i], innerSchema.fieldList[i], innerSchema.fieldMap[innerSchema.fieldList[i]]->clone());
          foundField = true;
          break;
        }
      }
      if (!foundField) {
        throw std::runtime_error("Field not found in inner schema");
      }
    }

  }

  ~ProjectScan() = default;

  bool getFirst() override;

  bool next() override;

  Tuple get() override;

  Schema& getSchema() override;
};

