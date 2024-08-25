#pragma once

#include "../query.h"
#include "../buffer.h"
#include "./scan.h"
#include "../HeapFileIterator.h"

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

// update opens 4 buffers
// 2 buffer for each heapfile iterator
class ModifyTableScan : public ModifyScan {
protected:
  std::shared_ptr<ResourceManager> rm;
  std::string filename;
  Schema schema;
  HeapFile::HeapFileIterator iter;
  HeapFile::HeapFileIterator pushIter;
  i32 currentSlot;

public:
  ModifyTableScan(std::string filenameInput, std::shared_ptr<ResourceManager> rmInput, Schema schemaInput) :
    rm{ rmInput }, filename{ filenameInput }, schema{ schemaInput }, iter{ HeapFile::HeapFileIterator(filename, rm) },
    pushIter{ filenameInput , rmInput }, currentSlot{ -1 } {

  }

  ~ModifyTableScan() = default;

  bool getFirst() override;

  bool next() override;

  Tuple get() override;

  Schema& getSchema() override;

  void update(UpdateStmt& tuple) override;

  bool deleteTuple() override;
};
