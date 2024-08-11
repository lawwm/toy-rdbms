#pragma once

#include "query.h"
#include "buffer.h"

class Scan {
public:
  virtual ~Scan() {};
  virtual bool getFirst() = 0;
  virtual bool next() = 0;
  virtual Tuple get() = 0;
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
};


class SelectScan : public Scan {
private:
  std::unique_ptr<Scan> scan;
  std::unique_ptr<Predicate> predicate;
  Schema schema;

public:
  SelectScan(std::unique_ptr<Scan> scan, std::unique_ptr<Predicate> predicate, Schema schema) :
    scan{ std::move(scan) }, predicate{ std::move(predicate) }, schema{ schema } {
  }

  ~SelectScan() = default;

  bool getFirst() override;

  bool next() override;

  Tuple get() override;
};

//class ProductScan : public Scan {
//
//};