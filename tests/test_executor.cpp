#include "catch.hpp"

#include "../src/executor.h"
#include "./test_utils.h"

TEST_CASE("Normal insert") {
  DeferDeleteFile deferDeleteFile({ "citizen", "schema" });
  {
    std::shared_ptr<ResourceManager> rm = std::make_shared<ResourceManager>(TEST_PAGE_SIZE, 10);

    auto createTable = R"(
    CREATE TABLE citizen(
                  name VARCHAR(30),
                  employment CHAR(20),                    
                  age INT
                );
  )";

    auto insert = R"(
    INSERT INTO citizen 
    VALUES 
      ("David", "Doctor", 27),
      ("Brian", "Engineer", 34),
      ("Catherine", "Teacher", 29),
      ("David", "Artist", 41),
      ("Emma", "Nurse", 31),

      ("Sophia", "Scientist", 28),
      ("James", "Lawyer", 39),
      ("Olivia", "Chef", 25),
      ("Liam", "Architect", 33),
      ("Mason", "Photographer", 30),

      ("Isabella", "Designer", 26),
      ("Lucas", "Pilot", 38),
      ("Mia", "Journalist", 24),
      ("Ethan", "Pharmacist", 37),
      ("Ava", "Dentist", 32),

      ("Madison", "Journalist", 29),
      ("Gabriel", "Photographer", 34),
      ("Sofia", "Technician", 30),
      ("Samuel", "Musician", 31),
      ("Layla", "Librarian", 39),

      ("Carter", "Civil Servant", 28),
      ("Aria", "Pharmacist", 32),
      ("Jayden", "Chef", 40),
      ("Riley", "Dentist", 37),
      ("John", "Engineer", 33),

      ("Lily", "Scientist", 25),
      ("Owen", "Lawyer", 36),
      ("Eleanor", "Photographer", 27),
      ("Julian", "Architect", 34),
      ("Lincoln", "Technician", 31),

      ("Mila", "Designer", 28),
      ("Thomas", "Chef", 39),
      ("Ariana", "Librarian", 26),
      ("Hudson", "Photographer", 33),
      ("Claire", "Software Developer", 27),

      ("Adam", "Pharmacist", 35),
      ("Skylar", "Mechanic", 24),
      ("Kennedy", "Librarian", 29),
      ("Miles", "Carpenter", 41),
      ("Samantha", "Teacher", 28),

      ("Zachary", "Dentist", 31),
      ("Vera", "Civil Servant", 32)
      ;
      )";

    Executor executor(rm);
    executor.execute(createTable);
    executor.execute(insert);

    auto [resultTuple, msg] = executor.execute("SELECT * FROM citizen;");

    REQUIRE(resultTuple.size() == 42);
  }

}


TEST_CASE("Normal insert and then query with order by") {
  DeferDeleteFile deferDeleteFile({ "citizen", "schema" });
  {
    std::shared_ptr<ResourceManager> rm = std::make_shared<ResourceManager>(TEST_PAGE_SIZE, 10);

    auto createTable = R"(
    CREATE TABLE citizen(
                  name VARCHAR(40),
                  employment CHAR(50),                    
                  age INT
                );
  )";

    auto insert = R"(
    INSERT INTO citizen
    VALUES
      ("David", "Doctor", 27),
      ("Brian", "Engineer", 34),
      ("Catherine", "Teacher", 29),
      ("David", "Artist", 41),
      ("Emma", "Nurse", 31),
      ("Sophia", "Scientist", 28),
      ("James", "Lawyer", 39),
      ("Olivia", "Chef", 25),
      ("Liam", "Architect", 33),
      ("Mason", "Photographer", 30),
      ("Isabella", "Designer", 26),
      ("Lucas", "Pilot", 38),
      ("Mia", "Journalist", 24),
      ("Ethan", "Pharmacist", 37),
      ("Ava", "Dentist", 32),
      ("Madison", "Journalist", 29),
      ("Gabriel", "Photographer", 34),
      ("Sofia", "Technician", 30),
      ("Samuel", "Musician", 31),
      ("Layla", "Librarian", 39),
      ("Carter", "Civil Servant", 28),
      ("Aria", "Pharmacist", 32),
      ("Jayden", "Chef", 40),
      ("Riley", "Dentist", 37),
      ("John", "Engineer", 33),
      ("Lily", "Scientist", 25),
      ("Owen", "Lawyer", 36),
      ("Eleanor", "Photographer", 27),
      ("Julian", "Architect", 34),
      ("Lincoln", "Technician", 31),
      ("Mila", "Designer", 28),
      ("Thomas", "Chef", 39),
      ("Ariana", "Librarian", 26),
      ("Hudson", "Photographer", 33),
      ("Claire", "Software Developer", 27),
      ("Adam", "Pharmacist", 35),
      ("Skylar", "Mechanic", 24),
      ("Kennedy", "Librarian", 29),
      ("Miles", "Carpenter", 41),
      ("Samantha", "Teacher", 28),
      ("Zachary", "Dentist", 31),
      ("Vera", "Civil Servant", 32),
      ("Grace", "Accountant", 30),
      ("Landon", "Consultant", 42),
      ("Ellie", "Veterinarian", 29),
      ("Noah", "Plumber", 35),
      ("Charlotte", "Marketing Manager", 33),
      ("Benjamin", "Electrician", 37),
      ("Scarlett", "Data Analyst", 26),
      ("Jack", "Police Officer", 38),
      ("Victoria", "Financial Analyst", 31),
      ("Henry", "Firefighter", 40),
      ("Emily", "Therapist", 28),
      ("Aiden", "Graphic Designer", 27),
      ("Luna", "Software Engineer", 32),
      ("Wyatt", "Systems Analyst", 36),
      ("Chloe", "UX Designer", 29),
      ("Sebastian", "Cybersecurity Specialist", 41),
      ("Zoe", "Environmental Engineer", 30),
      ("Daniel", "Web Developer", 35),
      ("Violet", "Architect", 33),
      ("Matthew", "Construction Manager", 39),
      ("Nora", "HR Manager", 28),
      ("Joseph", "IT Manager", 42),
      ("Hazel", "Operations Manager", 31),
      ("Caleb", "Mechanical Engineer", 37),
      ("Aurora", "Interior Designer", 26),
      ("Eli", "Biologist", 36),
      ("Penelope", "Product Manager", 29),
      ("Andrew", "Chemist", 34),
      ("Lucy", "Supply Chain Manager", 32),
      ("Leo", "Economist", 41),
      ("Stella", "Pharmacologist", 27),
      ("Isaac", "Physicist", 38),
      ("Harper", "Software Tester", 30),
      ("Nathan", "Robotics Engineer", 33),
      ("Avery", "Network Engineer", 35),
      ("Christian", "Database Administrator", 40),
      ("Leah", "Civil Engineer", 29),
      ("Hunter", "Project Manager", 31),
      ("Sophie", "Digital Marketing Specialist", 28),
      ("Aaron", "Biomedical Engineer", 36),
      ("Addison", "Speech Therapist", 33),
      ("Luke", "Surgeon", 39),
      ("Audrey", "Radiologist", 27),
      ("Jonathan", "Statistician", 34),
      ("Savannah", "Technical Writer", 30),
      ("Cameron", "Geneticist", 37),
      ("Piper", "Quality Assurance Specialist", 29),
      ("Isaiah", "Software Architect", 42),
      ("Ruby", "Research Scientist", 31),
      ("Joshua", "Aerospace Engineer", 38),
      ("Sadie", "Social Worker", 26),
      ("Anthony", "Operations Research Analyst", 35),
      ("Grace", "Graphic Designer", 28),
      ("Connor", "Electrical Engineer", 40),
      ("Ariana", "Physician Assistant", 32),
      ("Dylan", "Chemical Engineer", 33),
      ("Willow", "Human Resources Specialist", 29),
      ("Adrian", "Nuclear Engineer", 37),
      ("Claire", "Medical Scientist", 30),
      ("Asher", "Mechanical Technician", 41),
      ("Riley", "Dentist", 31),
      ("Lincoln", "Technician", 36),
      ("Liam", "Architect", 34);
      )";

    Executor executor(rm);
    executor.execute(createTable);
    auto [rows, msgrows] = executor.execute(insert);

    auto [resultTuple, msg] = executor.execute("SELECT * FROM citizen ORDER BY citizen.age ASC;");

    REQUIRE(resultTuple.size() == 105);
  }

}

TEST_CASE("Normal insert and then delete") {
  DeferDeleteFile deferDeleteFile({ "citizen", "schema" });
  {
    std::shared_ptr<ResourceManager> rm = std::make_shared<ResourceManager>(TEST_PAGE_SIZE, 10);

    auto createTable = R"(
    CREATE TABLE citizen(
                  name VARCHAR(30),
                  employment CHAR(20),                    
                  age INT
                );
  )";

    auto insert = R"(
    INSERT INTO citizen 
    VALUES 
     ("David", "Doctor", 27),
      ("Brian", "Engineer", 34),
      ("David", "Artist", 41),
      ("Emma", "Nurse", 31),
      ("Miles", "Carpenter", 41)
      ;
      )";

    auto deleteStmt = R"(
      DELETE FROM citizen WHERE citizen.age > 40;
    )";


    // number of citizens at the start should be equal to 5
    Executor executor(rm);
    executor.execute(createTable);
    executor.execute(insert);
    auto [beforeResultTuple, beforeMsg] = executor.execute("SELECT * FROM citizen;");
    REQUIRE(beforeResultTuple.size() == 5);

    // number of citizens after delete should be equal to 3
    executor.execute(deleteStmt);
    auto [afterResultTuple, msg] = executor.execute("SELECT * FROM citizen;");
    REQUIRE(afterResultTuple.size() == 3);
  }

}

TEST_CASE("Normal insert and then update") {
  DeferDeleteFile deferDeleteFile({ "citizen", "schema" });
  {
    std::shared_ptr<ResourceManager> rm = std::make_shared<ResourceManager>(TEST_PAGE_SIZE, 10);

    auto createTable = R"(
    CREATE TABLE citizen(
                  name VARCHAR(30),
                  employment CHAR(20),                    
                  age INT
                );
  )";

    auto insert = R"(
    INSERT INTO citizen 
    VALUES 
     ("David", "Doctor", 27),
      ("Brian", "Engineer", 34),
      ("David", "Artist", 41),
      ("Emma", "Nurse", 31),
      ("Miles", "Carpenter", 41)
      ;
      )";

    auto updateStmt = R"(
      UPDATE citizen SET employment = "Programmer" WHERE citizen.age > 40;
    )";
    auto updateStmt2 = R"(
      UPDATE citizen SET employment = "Unemployed" WHERE citizen.age <= 40;
    )";

    // number of citizens at the start should be equal to 5
    Executor executor(rm);
    executor.execute(createTable);
    executor.execute(insert);
    auto [beforeResultTuple, beforeMsg] = executor.execute("SELECT * FROM citizen;");
    REQUIRE(beforeResultTuple.size() == 5);

    // number of citizens after update should be equal to 5
    executor.execute(updateStmt);
    executor.execute(updateStmt2);
    auto [afterResultTuple, msg] = executor.execute("SELECT * FROM citizen;");

    REQUIRE(afterResultTuple.size() == 5);
    for (auto& tuple : afterResultTuple) {
      if (tuple.fields[2]->getConstant().num > 40) {
        REQUIRE(tuple.fields[1]->getConstant().str == "Programmer");
      }
      else {
        REQUIRE(tuple.fields[1]->getConstant().str == "Unemployed");
      }
    }
  }
}




TEST_CASE("Create two tables, populate them and query a join") {
  DeferDeleteFile deferDeleteFile({ "employees", "departments", "schema" });
  {
    std::shared_ptr<ResourceManager> rm = std::make_shared<ResourceManager>(TEST_PAGE_SIZE, 10);

    auto createTable1 = R"(
    CREATE TABLE employees (
        employee_id INT,
        employee_name VARCHAR(50),
        department_id INT,
        job_title VARCHAR(50)
      );
    )";
    auto createTable2 = R"(
    CREATE TABLE departments (
        department_id INT,
        department_name CHAR(50),
        location VARCHAR(50),
        manager_name VARCHAR(50),
        budget INT
      );
    )";

    auto insertTable1 = R"(
      INSERT INTO employees (employee_id, employee_name, department_id, job_title) 
        VALUES 
        (1, 'Alice Johnson', 101, 'Software Engineer'),
        (2, 'Bob Smith', 102, 'Project Manager'),
        (3, 'Carol White', 103, 'Data Analyst'),
        (4, 'David Brown', 101, 'Quality Assurance'),
        (5, 'Eve Davis', 104, 'DevOps Engineer'),
        (6, 'Frank Clark', 105, 'UI/UX Designer'),
        (7, 'Grace Lee', 102, 'Business Analyst'),
        (8, 'Hank Green', 103, 'Database Administrator'),
        (9, 'Ivy Walker', 104, 'Product Manager'),
        (10, 'Jack Harris', 105, 'Network Engineer');
      )";

    auto insertTable2 = R"(
    INSERT INTO departments (department_id, department_name, location, manager_name, budget) 
      VALUES 
      (101, 'Engineering', 'New York', 'Michael Scott', 1000000),
      (102, 'Management', 'San Francisco', 'Dwight Schrute', 1500000),
      (103, 'Data Science', 'Boston', 'Jim Halpert', 1200000),
      (104, 'Operations', 'Chicago', 'Pam Beesly', 900000),
      (105, 'Design', 'Los Angeles', 'Angela Martin', 800000),
      (106, 'Marketing', 'Miami', 'Ryan Howard', 600000),
      (107, 'Sales', 'Houston', 'Stanley Hudson', 700000),
      (108, 'Support', 'Phoenix', 'Phyllis Vance', 500000),
      (109, 'HR', 'Seattle', 'Toby Flenderson', 400000),
      (110, 'Legal', 'Denver', 'Jan Levinson', 1100000);
    )";

    auto query = R"(
      SELECT employee_name, department_name, location, job_title
      FROM 
      employees 
      JOIN departments
      ON employees.department_id = departments.department_id
      WHERE departments.budget >= 1200000;
    )";

    Executor executor(rm);
    executor.execute(createTable1);
    executor.execute(createTable2);
    executor.execute(insertTable1);
    executor.execute(insertTable2);
    auto [resultTuples, msg] = executor.execute(query);

    Schema schema;

    // to do, add ordering in order to test properly
    schema.addField("employees", "employee_name", std::make_unique<ReadVarCharField>());
    schema.addField("departments", "department_name", std::make_unique<ReadFixedCharField>(50));
    schema.addField("departments", "location", std::make_unique<ReadVarCharField>());
    schema.addField("employees", "job_title", std::make_unique<ReadVarCharField>());
    std::vector<std::vector<Token>> listOfListOfTokens{
      {ttoken("Carol White"), ttoken("Data Science"), ttoken("Boston"), ttoken("Data Analyst")},
      {ttoken("Bob Smith"), ttoken("Management"), ttoken("San Francisco"), ttoken("Project Manager")},
    };
    std::vector<Tuple> expectedTuples;
    for (auto tokenList : listOfListOfTokens) {
      expectedTuples.push_back(schema.createTuple(tokenList));
    }

    for (int i = 0; i < expectedTuples.size(); i++) {
      auto& expectedTuple = expectedTuples[i];
      auto& resultTuple = resultTuples[i];
      for (int j = 0; j < expectedTuple.fields.size(); j++) {
        auto expected = expectedTuple.fields[j]->getConstant();
        auto result = resultTuple.fields[j]->getConstant();
        REQUIRE(expected == result);
      }
    }
  }

}

// update with join

// delete with join

// insert very very large number of tuples

// check that order by works.

// check that order by multiple columns works.

// semantic analysis tuple too long

//