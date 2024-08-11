#include "catch.hpp"

#include "../src/executor.h"

TEST_CASE("test fuck u 1") {
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
  ("Aiden", "Musician", 29),
  ("Amelia", "Librarian", 40),
  ("Henry", "Carpenter", 36),
  ("Alexander", "Technician", 28),
  ("Ella", "Engineer", 34),
  ("Michael", "Software Developer", 27),
  ("Grace", "Civil Servant", 31),
  ("Daniel", "Chef", 39),
  ("Chloe", "Accountant", 30),
  ("Matthew", "Artist", 25),
  ("Emily", "Doctor", 33),
  ("David", "Mechanic", 29),
  ("Avery", "Veterinarian", 32),
  ("Jack", "Scientist", 37),
  ("Scarlett", "Pilot", 26),
  ("Sebastian", "Teacher", 35),
  ("Zoe", "Engineer", 28),
  ("Benjamin", "Lawyer", 41),
  ("Harper", "Dentist", 36),
  ("Elijah", "Nurse", 27),
  ("Abigail", "Artist", 24),
  ("William", "Architect", 38),
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
  ("Hannah", "Teacher", 31),
  ("Wyatt", "Veterinarian", 28),
  ("Lillian", "Accountant", 39),
  ("Levi", "Designer", 26),
  ("Natalie", "Software Developer", 30),
  ("Isaac", "Technician", 29),
  ("Brooklyn", "Carpenter", 41),
  ("Nathan", "Musician", 35),
  ("Savannah", "Journalist", 24),
  ("Caleb", "Pilot", 38),
  ("Victoria", "Civil Servant", 33),
  ("Ryan", "Nurse", 27),
  ("Aubrey", "Dentist", 32),
  ("Asher", "Architect", 37),
  ("Penelope", "Teacher", 26),
  ("Christopher", "Lawyer", 40),
  ("Camila", "Engineer", 29),
  ("Joshua", "Scientist", 36),
  ("Nora", "Veterinarian", 30),
  ("Andrew", "Accountant", 34),
  ("Hazel", "Artist", 25),
  ("Lincoln", "Technician", 31),
  ("Mila", "Designer", 28),
  ("Thomas", "Chef", 39),
  ("Ariana", "Librarian", 26),
  ("Hudson", "Photographer", 33),
  ("Claire", "Software Developer", 27),
  ("Adam", "Pharmacist", 35),
  ("Skylar", "Mechanic", 24),
  ("Leo", "Carpenter", 40),
  ("Violet", "Teacher", 32),
  ("Nathaniel", "Doctor", 29),
  ("Stella", "Civil Servant", 37),
  ("Eli", "Lawyer", 30),
  ("Paisley", "Dentist", 28),
  ("Joseph", "Scientist", 34),
  ("Aurora", "Journalist", 31),
  ("Luke", "Engineer", 27),
  ("Bella", "Nurse", 36),
  ("David", "Architect", 33),
  ("Lucy", "Designer", 25),
  ("Ezra", "Technician", 39),
  ("Luna", "Pilot", 26),
  ("Aaron", "Artist", 38),
  ("Evelyn", "Software Developer", 30),
  ("Cameron", "Photographer", 35),
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
