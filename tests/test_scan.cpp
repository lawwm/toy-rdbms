#include "catch.hpp"

#include "../src/scan.h"
#include "./test_utils.h"



TEST_CASE("Insert tuple") {
  const std::string fileName = "employee";
  DeferDeleteFile deferDeleteFile(fileName);
  {
    std::shared_ptr<ResourceManager> rm = std::make_shared<ResourceManager>(PAGE_SIZE_M, 10);

    HeapFile::createHeapFile(*rm, fileName);

    // Create schema
    Schema schema;
    schema.addField(fileName, "name", std::make_unique<ReadVarCharField>());
    schema.addField(fileName, "employment", std::make_unique<ReadFixedCharField>(20));
    schema.addField(fileName, "age", std::make_unique<ReadIntField>());

    // Create tokens
    std::vector<std::vector<Token>> listOfListOfTokens{
    { ttoken("Alicia"), ttoken("Doctor"), ttoken(27) },
    { ttoken("Brian"), ttoken("Engineer"), ttoken(34) },
    { ttoken("Catherine"), ttoken("Teacher"), ttoken(29) },
    { ttoken("David"), ttoken("Artist"), ttoken(41) },
    { ttoken("Emma"), ttoken("Lawyer"), ttoken(32) },
    { ttoken("Frank"), ttoken("Scientist"), ttoken(38) },
    { ttoken("Grace"), ttoken("Writer"), ttoken(25) },
    { ttoken("Henry"), ttoken("Chef"), ttoken(30) },
    { ttoken("Irene"), ttoken("Nurse"), ttoken(28) },
    { ttoken("Jack"), ttoken("Architect"), ttoken(36) }
    };

    // Create tuples and insert into the table
    // Ensure can write into files
    std::vector<Tuple> writeTuples;
    for (auto tokenList : listOfListOfTokens) {
      writeTuples.push_back(schema.createTuple(tokenList));
    }
    // tuples will be sorted by length within this function
    HeapFile::insertTuples(rm, fileName, writeTuples);


    // Ensure can read from file
    std::unique_ptr<Scan> tableScan = std::make_unique<TableScan>(fileName, rm, schema);
    std::vector<Tuple> readTuples;
    tableScan->getFirst();
    while (tableScan->next()) {
      auto result = tableScan->get();
      readTuples.push_back(std::move(result));
    }

    // Ensure that the tuples are the same
    REQUIRE(readTuples.size() == writeTuples.size());


    for (int i = 0; i < writeTuples.size(); i++) {
      auto& writeTuple = writeTuples[i];
      auto& readTuple = readTuples[i];
      for (int j = 0; j < writeTuple.fields.size(); j++) {
        auto writtenConstant = writeTuple.fields[j]->getConstant();
        auto readConstant = readTuple.fields[j]->getConstant();
        REQUIRE(writtenConstant == readConstant);
      }
    }

    // Ensure you can do a selection on a table scan
    auto predicate = std::make_unique<Predicate>(
      std::make_unique<Term>(TermOperand::GREATER_EQUAL, std::make_unique<Field>(fileName, "age"), std::make_unique<Constant>(38))
    );
    SelectScan selectedScan(std::move(tableScan), std::move(predicate));

    std::vector<Tuple> selectedTuples;
    selectedScan.getFirst();
    while (selectedScan.next()) {
      auto result = selectedScan.get();
      selectedTuples.push_back(std::move(result));
    }

    REQUIRE(selectedTuples.size() == 2);
  }
}

// i want to create tuples without the make_unique boiler plate bs.