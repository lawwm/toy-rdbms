#pragma once


#include <variant>

#include "parser.h"
#include "query.h"
#include "buffer.h"

class Executor {
public:
  Executor(std::shared_ptr<ResourceManager> resourceManager) : resourceManager{ resourceManager } {

  }

  void execute(std::string sqlStmt) {
    Parser parser(sqlStmt);
    std::variant<Query, Insert, Schema> stmt = parser.parseStatement();
    if (std::holds_alternative<Query>(stmt)) {
      // QUERY
      auto queryStmt = std::get<Query>(stmt);
      auto schemaMap = getSchemaFromTableName(queryStmt.tables, resourceManager);
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
      HeapFile::insertTuples(resourceManager, schema, tuples);

    }
    else if (std::holds_alternative<Schema>(stmt)) {
      // CREATE
      HeapFile::createHeapFile(*resourceManager, SCHEMA_TABLE, 8);
      auto schema = std::get<Schema>(stmt);
      auto tuples = schema.createSchemaTuple();
      HeapFile::createHeapFile(*resourceManager, schema.filename, 8);
      HeapFile::insertTuples(resourceManager, schemaTable, tuples);

    }

  }

private:
  std::shared_ptr<ResourceManager> resourceManager;
};