#include "ProjectScan.h"

bool ProjectScan::getFirst()
{
  return this->scan->getFirst();
}

bool ProjectScan::next()
{
  return this->scan->next();
}

Tuple ProjectScan::get()
{
  auto innerTuple = scan->get();
  std::vector<std::unique_ptr<WriteField>> output;
  for (u32 index : mapToInnerSchema) {
    output.push_back(std::move(innerTuple.fields[index]));
  }
  return Tuple(std::move(output));
}

Schema& ProjectScan::getSchema()
{
  return this->schema;
}

/**
  ProjectModifyScan
*/

bool ProjectModifyScan::getFirst()
{
  return this->scan->getFirst();
}

bool ProjectModifyScan::next()
{
  return this->scan->next();
}

Tuple ProjectModifyScan::get()
{
  auto innerTuple = scan->get();
  std::vector<std::unique_ptr<WriteField>> output;
  for (u32 index : mapToInnerSchema) {
    output.push_back(std::move(innerTuple.fields[index]));
  }
  return Tuple(std::move(output));
}

Schema& ProjectModifyScan::getSchema()
{
  return this->schema;
}

void ProjectModifyScan::update(UpdateStmt& tuple)
{
  this->scan->update(tuple);
}

bool ProjectModifyScan::deleteTuple()
{
  return this->scan->deleteTuple();
}