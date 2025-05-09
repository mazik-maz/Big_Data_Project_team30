-- Q4: Crime distribution by premise type
-- This query analyzes where crimes are most commonly occurring

USE team30_projectdb;

-- Drop the results table if it exists
DROP TABLE IF EXISTS q4_results;

-- Create table to store results
CREATE EXTERNAL TABLE q4_results (
    premise_type STRING,
    crime_count INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/team30/project/hive/warehouse/q4';

-- To not display table names with column names
SET hive.resultset.use.unique.column.names = false;

-- Insert data from our query
INSERT INTO q4_results
SELECT
    PREM_TYP_DESC as premise_type,
    COUNT(*) as crime_count
FROM nypd_complaints_part
WHERE PREM_TYP_DESC IS NOT NULL
GROUP BY PREM_TYP_DESC
ORDER BY crime_count DESC;

-- Export results to a CSV file for visualization
INSERT OVERWRITE DIRECTORY '/user/team30/project/output/q4'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM q4_results;

-- Display results
SELECT * FROM q4_results;