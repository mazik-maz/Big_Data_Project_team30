-- Q1: Crime distribution by borough and category
-- This query analyzes crime distribution across NYC boroughs and crime categories

USE team30_projectdb;

-- Drop the results table if it exists
DROP TABLE IF EXISTS q1_results;

-- Create table to store results
CREATE EXTERNAL TABLE q1_results (
    borough STRING,
    law_category STRING,
    crime_count INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/team30/project/hive/warehouse/q1';

-- To not display table names with column names
SET hive.resultset.use.unique.column.names = false;

-- Insert data from our query
INSERT INTO q1_results
SELECT
    BORO_NM as borough,
    LAW_CAT_CD as law_category,
    COUNT(*) as crime_count
FROM nypd_complaints_part
WHERE BORO_NM IS NOT NULL AND LAW_CAT_CD IS NOT NULL
GROUP BY BORO_NM, LAW_CAT_CD
ORDER BY borough, crime_count DESC;

-- Export results to a CSV file for visualization
INSERT OVERWRITE DIRECTORY '/user/team30/project/output/q1'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM q1_results;

-- Display results
SELECT * FROM q1_results;