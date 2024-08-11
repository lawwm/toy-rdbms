#include "catch.hpp"

#include "../src/parser.h"
#include <vector>
#include <memory>

TEST_CASE("Lexer succeeds for normal query") {
  auto s = R"(SELECT * 
            FROM 
            table;)";
  Lexer lexer(s);
  std::vector<Token> tokens;
  while (!lexer.isEOF()) {
    tokens.push_back(lexer.nextToken());
  }
  std::vector<Token> expected = { Token{SELECT, 1}, Token{STAR, 1}, Token{FROM, 2}, Token{IDENTIFIER, 3, "table"}, Token{SEMI_COLON, 3} };
  REQUIRE(tokens.size() == expected.size());
  for (int i = 0; i < tokens.size(); i++) {
    REQUIRE(tokens[i].tokenType == expected[i].tokenType);
    REQUIRE(tokens[i].line == expected[i].line);
    REQUIRE(tokens[i].lexeme == expected[i].lexeme);
  }
}

TEST_CASE("Lexer succeeds for more complicated query") {
  auto s = R"(SELECT name, ranking, major FROM 
            student
            JOIN school
            ON student.schoolId = school.id;)";
  Lexer lexer(s);
  std::vector<Token> tokens;
  while (!lexer.isEOF()) {
    tokens.push_back(lexer.nextToken());
  }
  std::vector<Token> expected = {
   Token{SELECT, 1}, Token{IDENTIFIER, 1, "name"}, Token{COMMA, 1}, Token{IDENTIFIER, 1, "ranking"}, Token{COMMA, 1}, Token{IDENTIFIER, 1, "major"},
   Token{FROM, 1}, Token{IDENTIFIER, 2, "student"}, Token{JOIN, 3}, Token{IDENTIFIER, 3, "school"}, Token{ON, 4}, Token{IDENTIFIER, 4, "student"},
   Token{DOT, 4}, Token{IDENTIFIER, 4, "schoolId"}, Token{EQUAL, 4}, Token{IDENTIFIER, 4, "school"}, Token{DOT, 4}, Token{IDENTIFIER, 4, "id"}, Token{SEMI_COLON, 4}
  };
  REQUIRE(tokens.size() == expected.size());
  for (int i = 0; i < tokens.size(); i++) {
    //REQUIRE(tokens[i].tokenType == expected[i].tokenType);
     //REQUIRE(tokens[i] == expected[i]);
     //REQUIRE(tokens[i] == expected[i]);
    REQUIRE(tokens[i].tokenType == expected[i].tokenType);
    REQUIRE(tokens[i].line == expected[i].line);
  }
}

TEST_CASE("Parser succeeds for 3 table join without parenthesis") {
  auto cmd = R"(SELECT name, school.ranking, student.major, 3, "toy" FROM 
            (student
            JOIN school
            ON student.schoolId = school.id)
            JOIN courses
            ON courses.schoolId = school.id
            WHERE (school.area = "tampines" AND name != "jimmy") OR courses.credits >= 4;)";

  Parser parser(cmd);
  auto query = parser.parseQuery();

  std::vector<TableValue*> expectedSelectList = {
    new Field("name", "name"),
    new Field("school", "ranking"),
    new Field("student", "major"),
    new Constant(3),
    new Constant("toy"),
  };


  REQUIRE(query.selectFields.size() == expectedSelectList.size());
  for (int i = 0; i < query.selectFields.size(); i++) {
    TableValue* lhs = query.selectFields[i].get();
    TableValue* rhs = expectedSelectList[i];
    REQUIRE(*lhs == rhs);
    free(expectedSelectList[i]);
  }

  std::vector<std::string> expectedJoinTable = { "student", "school", "courses" };
  REQUIRE(query.joinTable.size() == expectedJoinTable.size());

  // create predicate for student.schoolId = school.id
  auto firstPredicate = std::make_unique<Predicate>(
    std::make_unique<Term>(TermOperand::EQUAL, std::make_unique<Field>("student", "schoolId"), std::make_unique<Field>("school", "id"))
  );

  auto secondPredicate = std::make_unique<Predicate>(
    std::make_unique<Term>(TermOperand::EQUAL, std::make_unique<Field>("courses", "schoolId"), std::make_unique<Field>("school", "id"))
  );


  auto inner1 = std::make_unique<Predicate>(std::make_unique<Term>(TermOperand::EQUAL, std::make_unique<Field>("school", "area"), std::make_unique<Constant>("tampines")));
  auto inner2 = std::make_unique<Predicate>(std::make_unique<Term>(TermOperand::NOT_EQUAL, std::make_unique<Field>("name", "name"), std::make_unique<Constant>("jimmy")));
  auto inner3 = std::make_unique<Predicate>(std::make_unique<Term>(TermOperand::GREATER_EQUAL, std::make_unique<Field>("courses", "credits"), std::make_unique<Constant>(4)));
  auto  thirdPredicate = std::make_unique<Predicate>(
    PredicateOperand::OR,
    std::make_unique<Predicate>(PredicateOperand::AND,
      std::move(inner1),
      std::move(inner2)),
    std::move(inner3)
  );

  REQUIRE(*firstPredicate == *query.predicate[0]);
  REQUIRE(*secondPredicate == *query.predicate[1]);
  REQUIRE(*thirdPredicate == *query.predicate[2]);
}

TEST_CASE("Parser succeeds for create table") {
  auto cmd = R"(CREATE TABLE citizen(
                  name VARCHAR(30),
                  employment CHAR(20),                    
                  age INT
                );)";

  Parser parser(cmd);
  auto schema = parser.parseCreate();
  std::vector<std::string> expectedFieldNames{ "name", "employment", "age" };
  REQUIRE(schema.filename == "citizen");
  for (int i = 0; i < schema.fieldList.size(); ++i) {
    REQUIRE(schema.fieldList[i] == expectedFieldNames[i]);
  }

  REQUIRE(dynamic_cast<ReadVarCharField*>(schema.fieldMap["name"].get()) != nullptr);
  REQUIRE(dynamic_cast<ReadFixedCharField*>(schema.fieldMap["employment"].get()) != nullptr);
  REQUIRE(dynamic_cast<ReadIntField*>(schema.fieldMap["age"].get()) != nullptr);
}

TEST_CASE("Parser succeeds for insert into table") {
  auto cmd = R"(INSERT INTO citizen(name, employment, age) 
                VALUES
                  ("David", "Doctor", 27),
                  ("Brian", "Engineer", 34),
                  ("Catherine", "Teacher", 29),
                  ("David", "Artist", 41);)";

  Parser parser(cmd);
  auto insert = parser.parseInsert();
  REQUIRE(insert.table == "citizen");
  REQUIRE(insert.fields[0] == "name");
  REQUIRE(insert.fields[1] == "employment");
  REQUIRE(insert.fields[2] == "age");
  REQUIRE(insert.values.size() == 4);
}