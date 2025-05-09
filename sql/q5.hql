-- Q5: Demographic analysis of crime victims
-- This query analyzes victim demographics by age group, race, and sex

USE team30_projectdb;

-- Drop the results table if it exists
DROP TABLE IF EXISTS q5_results;

-- Create table to store results
CREATE EXTERNAL TABLE q5_results (
    vic_age_group STRING,
    vic_race STRING,
    vic_sex STRING,
    crime_count INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/team30/project/hive/warehouse/q5';

-- To not display table names with column names
SET hive.resultset.use.unique.column.names = false;

-- Insert data from our query
INSERT INTO q5_results
SELECT
    VIC_AGE_GROUP as vic_age_group,
    VIC_RACE as vic_race,
    VIC_SEX as vic_sex,
    COUNT(*) as crime_count
FROM nypd_complaints_part
WHERE VIC_AGE_GROUP IS NOT NULL
  AND VIC_RACE IS NOT NULL
  AND VIC_SEX IS NOT NULL
  AND VIC_AGE_GROUP != '(null)'
  AND VIC_RACE != '(null)'
  AND VIC_SEX != '(null)'
GROUP BY VIC_AGE_GROUP, VIC_RACE, VIC_SEX
ORDER BY crime_count DESC;

-- Export results to a CSV file for visualization
INSERT OVERWRITE DIRECTORY '/user/team30/project/output/q5'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM q5_results;

-- Display results
SELECT * FROM q5_results;