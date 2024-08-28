#pragma once


#include "../query.h"
#include "./scan.h"



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


class ProductModifyScan : public ModifyScan {
private:
  std::unique_ptr<ModifyScan> leftScan;
  std::unique_ptr<Scan> rightScan;
  Schema schema;
  // this map tells how to get the fields
  // (lhs or rhs, index).
  // lhs or rhs shows whether lhs or rhs contains the field, 0 -> lhs, 1 -> rhs
  // index is the index of the field in the lhs/rhs schema

public:
  ProductModifyScan(std::unique_ptr<ModifyScan> leftScanInput, std::unique_ptr<Scan> rightScanInput) :
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

  ~ProductModifyScan() = default;

  bool getFirst() override;

  bool next() override;

  Tuple get() override;

  Schema& getSchema() override;

  void update(UpdateStmt& tuple) override;

  bool deleteTuple() override;
};