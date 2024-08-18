#pragma once

#include <memory>

#include "../query.h"
#include "../buffer.h"
#include "./scan.h"

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

class ProjectModifyScan : public ModifyScan {
private:
  std::unique_ptr<ModifyScan> scan;
  std::vector<u32> mapToInnerSchema;
  Schema schema;
public:
  ProjectModifyScan(std::unique_ptr<ModifyScan> inputScan, std::vector<std::unique_ptr<TableValue>>& fields) :
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

  ~ProjectModifyScan() = default;

  bool getFirst() override;

  bool next() override;

  Tuple get() override;

  Schema& getSchema() override;

  void update(UpdateStmt& tuple) override;
  bool deleteTuple() override;
};
