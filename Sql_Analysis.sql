--1. Query to count the number of animals of each type with outcomes (counting animals only once).
WITH AnimalsWithOutcomes AS (
    SELECT DISTINCT ai.Animal_ID, ai.Animal_Type
    FROM dim_Animal_Info ai
    JOIN fact_Outcome_Info fo ON ai.Animal_ID = fo.Animal_ID
)
SELECT Animal_Type, COUNT(*) AS Total_Count
FROM AnimalsWithOutcomes
GROUP BY Animal_Type
ORDER BY Total_Count DESC;



--2.
SELECT Animal_ID, COUNT(Outcome_ID) AS Outcome_Count
FROM fact_Outcome_Info
GROUP BY Animal_ID
HAVING COUNT(Outcome_ID) > 1;


--3.
SELECT TO_CHAR(DateTime, 'Month') AS Month, COUNT(*) AS OutcomeCount
FROM fact_Outcome_Info
GROUP BY Month
ORDER BY OutcomeCount DESC
LIMIT 5;



--4-1 
--Query to find the total number of kittens, adults, and seniors among cats whose outcome is "Adopted."
WITH CatAgeCategories AS (
    SELECT
        CASE
            WHEN ai.Animal_Type = 'Cat' AND ai.Age < 1 THEN 'Kitten'
            WHEN ai.Animal_Type = 'Cat' AND ai.Age >= 1 AND ai.Age <= 10 THEN 'Adult'
            WHEN ai.Animal_Type = 'Cat' AND ai.Age > 10 THEN 'Senior Cat'
            ELSE 'Other'
        END AS Age_Category,
        COUNT(*) AS Total_Count
    FROM dim_Animal_Info ai
    WHERE ai.Animal_Type = 'Cat'
    GROUP BY Age_Category
)
SELECT
    c.Age_Category,
    c.Total_Count
FROM CatAgeCategories c;



--4-2 
--Total number of "Adopted" cats in each age category (Kitten, Adult, Senior)
WITH AdoptedCatAgeCategories AS (
    SELECT
        CASE
            WHEN ai.Animal_Type = 'Cat' AND ai.Age < 1 THEN 'Kitten'
            WHEN ai.Animal_Type = 'Cat' AND ai.Age >= 1 AND ai.Age <= 10 THEN 'Adult'
            WHEN ai.Animal_Type = 'Cat' AND ai.Age > 10 THEN 'Senior Cat'
            ELSE 'Other'
        END AS Age_Category,
        COUNT(*) AS Adopted_Count
    FROM dim_Animal_Info ai
    JOIN fact_Outcome_Info fo ON ai.Animal_ID = fo.Animal_ID
    WHERE ai.Animal_Type = 'Cat' AND fo.Outcome_Type = 'Adopted'
    GROUP BY Age_Category
)
-- Select the count of "Adopted" cats in each age category
SELECT
    Age_Category,
    Adopted_Count
FROM AdoptedCatAgeCategories;




--5. Query to calculate the cumulative total of outcomes by date
WITH CumulativeOutcomeCounts AS (
    SELECT
        DateTime,
        SUM(COUNT(*)) OVER (ORDER BY DateTime) AS Cumulative_Count
    FROM fact_Outcome_Info
    GROUP BY DateTime
)
SELECT
    DateTime,
    Cumulative_Count
FROM CumulativeOutcomeCounts
ORDER BY DateTime;

