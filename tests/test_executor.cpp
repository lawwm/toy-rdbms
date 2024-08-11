#include "catch.hpp"

#include "../src/executor.h"
#include "./test_utils.h"

TEST_CASE("Normal insert") {
  DeferDeleteFile deferDeleteFile("citizen");
  {
    std::shared_ptr<ResourceManager> rm = std::make_shared<ResourceManager>(PAGE_SIZE_M, 10);

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

    REQUIRE(1 == 1);
  }

}



TEST_CASE("Create two tables, populate them and query a join") {
  DeferDeleteFile deferDeleteFile({ "employees", "departments" });
  {
    std::shared_ptr<ResourceManager> rm = std::make_shared<ResourceManager>(PAGE_SIZE_M, 10);

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
      WHERE departments.budget >= 900000;
    )";

    Executor executor(rm);
    executor.execute(createTable1);
    executor.execute(createTable2);
    executor.execute(insertTable1);
    executor.execute(insertTable2);
    executor.execute(query);
  }

  //try {
  //  Executor executor(rm);
  //  for (auto& cmd : { createTable1, createTable2, insertTable1, insertTable2 }) {
  //    executor.execute(cmd);
  //  }

  //  executor.execute(query);
  //}
  //catch (...) {
  //  int a = 0;
  //}

}