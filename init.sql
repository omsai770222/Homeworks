DROP TABLE IF EXISTS dim_Date;
DROP TABLE IF EXISTS dim_Animal_Info;
DROP TABLE IF EXISTS dim_Animal_char;
DROP TABLE IF EXISTS fact_Outcome_Info;





CREATE TABLE dim_Date (
    DateTime TIMESTAMP NOT NULL PRIMARY KEY,
    Time TIME,
    Month VARCHAR(50),
    Day INT,
    Year INT,
    Is_Weekend BOOLEAN
);



CREATE TABLE dim_Animal_Info
(
  Animal_ID        VARCHAR(255) PRIMARY KEY NOT NULL,
  Name             VARCHAR(255),
  Date_of_Birth    DATE NOT NULL,
  Animal_Type      VARCHAR(255),
  Age              FLOAT
);



CREATE TABLE dim_Animal_char
(
  Animal_ID              VARCHAR(255) PRIMARY KEY NOT NULL,
  Breed                  VARCHAR(255),
  Color                  VARCHAR(255),
  Sterilization_Status   VARCHAR(255),
  Sex                    VARCHAR(255)
);




CREATE TABLE fact_Outcome_Info
(
  Outcome_ID        SERIAL PRIMARY KEY,
  Animal_ID         VARCHAR(255),
  DateTime          TIMESTAMP REFERENCES dim_Date(DateTime),
  Outcome_Type      VARCHAR(255),
  Outcome_SubType   VARCHAR(255),
  CONSTRAINT fk_dimAnimal_Info FOREIGN KEY (Animal_ID) REFERENCES dim_Animal_Info(Animal_ID),
  CONSTRAINT fk_dimAnimal_char FOREIGN KEY (Animal_ID) REFERENCES dim_Animal_char(Animal_ID)
);
