
#include "./SelectScan.h"

bool SelectScan::getFirst()
{
  return this->scan->getFirst();
}

bool SelectScan::next()
{
  while (scan->next()) {
    auto tuple = scan->get();
    if (predicate->evaluate(tuple, scan->getSchema())) {
      return true;
    }
  }
  return false;
}

Tuple SelectScan::get()
{
  return this->scan->get();
}

Schema& SelectScan::getSchema()
{
  return this->scan->getSchema();
}

bool SelectModifyScan::getFirst()
{
  return this->scan->getFirst();
}

bool SelectModifyScan::next()
{
  while (scan->next()) {
    auto tuple = scan->get();
    if (predicate->evaluate(tuple, scan->getSchema())) {
      return true;
    }
  }
  return false;
}

Tuple SelectModifyScan::get()
{
  return this->scan->get();
}

Schema& SelectModifyScan::getSchema()
{
  return this->scan->getSchema();
}

void SelectModifyScan::update(UpdateStmt& tuple)
{
  this->scan->update(tuple);
}

bool SelectModifyScan::deleteTuple()
{
  return this->scan->deleteTuple();
}