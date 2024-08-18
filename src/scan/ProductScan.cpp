#include "ProductScan.h"

bool ProductScan::getFirst()
{
  return leftScan->getFirst()
    && leftScan->next()
    && rightScan->getFirst();
}

bool ProductScan::next()
{
  if (rightScan->next()) {
    return true;
  }
  else {
    rightScan->getFirst();
    return leftScan->next() && rightScan->next();
  }
}

Tuple ProductScan::get()
{
  auto lhs = leftScan->get();
  auto rhs = rightScan->get();
  std::vector<std::unique_ptr<WriteField>> output;
  for (auto& field : lhs.fields) {
    output.push_back(std::move(field));
  }
  for (auto& field : rhs.fields) {
    output.push_back(std::move(field));
  }

  return Tuple(std::move(output));
}

Schema& ProductScan::getSchema()
{
  return this->schema;
}


/***
ProductScan
**/

bool ProductModifyScan::getFirst()
{
  return leftScan->getFirst()
    && leftScan->next()
    && rightScan->getFirst();
}

bool ProductModifyScan::next()
{
  if (rightScan->next()) {
    return true;
  }
  else {
    rightScan->getFirst();
    return leftScan->next() && rightScan->next();
  }
}

Tuple ProductModifyScan::get()
{
  auto lhs = leftScan->get();
  auto rhs = rightScan->get();
  std::vector<std::unique_ptr<WriteField>> output;
  for (auto& field : lhs.fields) {
    output.push_back(std::move(field));
  }
  for (auto& field : rhs.fields) {
    output.push_back(std::move(field));
  }

  return Tuple(std::move(output));
}

Schema& ProductModifyScan::getSchema()
{
  return this->schema;
}

void ProductModifyScan::update(UpdateStmt& tuple)
{
  this->leftScan->update(tuple);
}

bool ProductModifyScan::deleteTuple()
{
  return this->leftScan->deleteTuple();
}