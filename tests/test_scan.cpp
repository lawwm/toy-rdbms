#include "catch.hpp"

#include "../src/scan.h"
#include "./test_utils.h"



TEST_CASE("Insert tuple") {
  const std::string fileName = "testFile";
  DeferDeleteFile deferDeleteFile(fileName);
  {
    std::shared_ptr<ResourceManager> rm = std::make_shared<ResourceManager>(PAGE_SIZE_M, 10);

    HeapFile::createHeapFile(*rm, fileName);

    Schema schema(fileName);
    schema.addField("name", std::make_unique<ReadVarCharField>());
    schema.addField("employment", std::make_unique<ReadFixedCharField>(20));
    schema.addField("age", std::make_unique<ReadIntField>());

    std::vector<std::unique_ptr<WriteField>> writeFields;
    writeFields.emplace_back(std::make_unique<VarCharField>("memes lol haha"));
    writeFields.emplace_back(std::make_unique<FixedCharField>(20, "fixed char"));
    writeFields.emplace_back(std::make_unique<IntField>(4));

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

    std::vector<Tuple> tuples;
    for (auto tokenList : listOfListOfTokens) {
      tuples.push_back(schema.createTuple(tokenList));
    }

    // write after free
    HeapFile::insertTuple(*rm, schema, tuples[0]);
    std::vector<std::unique_ptr<ReadField>> readFields;

    TableScan scan(fileName, rm, std::move(schema));
    scan.getFirst();
    scan.next();
    auto result = scan.get();
    std::vector<Constant> constants;
    for (int i = 0; i < result.fields.size(); i++) {
      constants.push_back(result.fields[i]->getConstant());
    }
    REQUIRE(constants[0] == Constant("Alicia"));
    REQUIRE(constants[1] == Constant("Doctor"));
    REQUIRE(constants[2] == Constant(27));
  }
}

// i want to create tuples without the make_unique boiler plate bs.