#pragma once


#include "../query.h"
#include "../buffer.h"
#include "./scan.h"

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

class SelectModifyScan : public ModifyScan {
private:
  std::unique_ptr<ModifyScan> scan;
  std::unique_ptr<Predicate> predicate;

public:
  SelectModifyScan(std::unique_ptr<ModifyScan> scan, std::unique_ptr<Predicate> predicate) :
    scan{ std::move(scan) }, predicate{ std::move(predicate) } {
  }

  ~SelectModifyScan() = default;

  bool getFirst() override;

  bool next() override;

  Tuple get() override;

  Schema& getSchema() override;

  void update(UpdateStmt& tuple) override;
  bool deleteTuple() override;
};
