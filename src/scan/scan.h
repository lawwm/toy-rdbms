#pragma once

#include "../query.h"
#include "../buffer.h"

class Scan {
public:
  virtual ~Scan() {};
  virtual bool getFirst() = 0;
  virtual bool next() = 0;
  virtual Tuple get() = 0;
  virtual Schema& getSchema() = 0;
};

class ModifyScan : public Scan {
public:
  virtual ~ModifyScan() {};
  virtual void update(UpdateStmt& tuple) = 0;
  virtual bool deleteTuple() = 0;
};
