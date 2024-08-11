#pragma once


#include <variant>

#include "parser.h"
#include "query.h"
#include "buffer.h"
#include "scan.h"

class Executor {
public:
  Executor(std::shared_ptr<ResourceManager> resourceManager) : resourceManager{ resourceManager } {
    if (!resourceManager->fm.doesFileExists(SCHEMA_TABLE)) {
      HeapFile::createHeapFile(*resourceManager, SCHEMA_TABLE, 8);
    }
  }

  std::unique_ptr<Scan> createBasicScan(Query& queryStmt) {
    auto schemaMap = getSchemaFromTableName(queryStmt.joinTable, resourceManager);
    std::string currTable = queryStmt.joinTable.at(0);
    std::unique_ptr<Scan> lhs = std::make_unique<TableScan>(currTable, resourceManager, schemaMap.at(currTable));

    // joining tables
    for (u32 i = 1; i < queryStmt.joinTable.size(); ++i) {
      std::string rhsTable = queryStmt.joinTable.at(i);
      std::unique_ptr<Scan> rhs = std::make_unique<TableScan>(rhsTable, resourceManager, schemaMap.at(rhsTable));
      auto temp = std::make_unique<ProductScan>(std::move(lhs), std::move(rhs));
      lhs = std::move(temp);
    }

    // selection
    std::unique_ptr<Predicate> pred;
    if (queryStmt.predicate.size() > 0) {
      pred = std::move(queryStmt.predicate.at(0));
      for (u32 i = 1; i < queryStmt.predicate.size(); ++i) {
        pred = std::make_unique<Predicate>(PredicateOperand::AND,
          std::move(pred),
          std::move(queryStmt.predicate.at(i)));
      }
    }
    lhs = std::make_unique<SelectScan>(std::move(lhs), std::move(pred));

    // projection
    lhs = std::make_unique<ProjectScan>(std::move(lhs), queryStmt.selectFields);

    return lhs;
  }

  void execute(std::string sqlStmt) {
    Parser parser(sqlStmt);
    std::variant<Query, Insert, Schema> stmt = parser.parseStatement();
    if (std::holds_alternative<Query>(stmt)) {
      // QUERY
      auto& queryStmt = std::get<Query>(stmt);
      auto scan = createBasicScan(queryStmt);
      scan->getFirst();
      while (scan->next()) {
        auto tuple = scan->get();
        int a = 0;
      }
    }
    else if (std::holds_alternative<Insert>(stmt)) {
      // INSERT
      auto insertStmt = std::get<Insert>(stmt);
      auto schemaMap = getSchemaFromTableName({ insertStmt.table }, resourceManager);
      auto schema = schemaMap.at(insertStmt.table);
      std::vector<Tuple> tuples;
      for (auto& tokenList : insertStmt.values) {
        tuples.push_back(schema.createTuple(tokenList));
      }
      HeapFile::insertTuples(resourceManager, insertStmt.table, tuples);

    }
    else if (std::holds_alternative<Schema>(stmt)) {
      // CREATE
      auto schema = std::get<Schema>(stmt);
      auto tuples = schema.createSchemaTuple();

      // check whether table already exists
      auto schemaMap = getSchemaFromTableName({ schema.tableList.at(0) }, resourceManager);
      if (schemaMap.size() > 0) {
        std::cout << "Table already exists\n";
        return;
      }
      // Base level schema, there can only be 1 and only 1 table
      HeapFile::createHeapFile(*resourceManager, schema.tableList.at(0), 8);
      HeapFile::insertTuples(resourceManager, SCHEMA_TABLE, tuples);

    }

  }

private:
  std::shared_ptr<ResourceManager> resourceManager;
};