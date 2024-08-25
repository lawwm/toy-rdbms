#pragma once

#include <atomic>
#include <unordered_map>
#include <string>
#include <memory>
#include "../common.h"
#include "./scan.h"
#include "./TableScan.h"
#include "../query.h"
#include "../HeapFileIterator.h"

// input -> some scan.
// output -> a table scan on the new table.
class TempTableScan : public Scan {
private:
  inline static std::atomic<u64> tempNameAtomic = 1;

  std::unique_ptr<Scan> scan;
  std::unique_ptr<Scan> materializedScan;
  std::shared_ptr<ResourceManager> resourceManager;
  std::string fileName;
public:
  TempTableScan(std::unique_ptr<Scan> inputScan, std::shared_ptr<ResourceManager> rm) :
    scan{ std::move(inputScan) }, materializedScan{ nullptr },
    resourceManager{ rm }, fileName{ "" } {
  }

  ~TempTableScan() {
    this->resourceManager->fm.deleteFile(this->fileName);
  }

  bool getFirst() override {
    if (materializedScan) {
      return materializedScan->getFirst();
    }
    else {
      // create a new table
      this->fileName = "temp_" + std::to_string(tempNameAtomic.fetch_add(1));
      // insert the tuples into the new table
      HeapFile::HeapFileIterator iter = HeapFile::HeapFileIterator(fileName, resourceManager);
      this->scan->getFirst();
      while (scan->next()) {
        iter.insertTuple(scan->get());
      }

      // create a table scan on the new table

      materializedScan = std::make_unique<TableScan>(fileName, resourceManager, scan->getSchema());
      return materializedScan->getFirst();
    }
  }

  bool next() override {
    return materializedScan->next();
  }

  Tuple get() override {
    return materializedScan->get();
  }

  Schema& getSchema() override {
    return this->scan->getSchema();
  }
};

class TableScanTemp : public TableScan {
private:
  std::string fileName;
  std::shared_ptr<ResourceManager> resourceManager;
public:
  TableScanTemp(std::string filename, std::shared_ptr<ResourceManager> rm, Schema schema) :
    TableScan(filename, rm, schema), fileName{ filename }, resourceManager{ rm } {
  }

  ~TableScanTemp() {
    this->resourceManager->fm.deleteFile(this->fileName);
  }
};