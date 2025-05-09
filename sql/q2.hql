-- Q2: Top 10 most common crime types
-- This query identifies the most frequent crimes in the dataset

USE team30_projectdb;

-- Drop the results table if it exists
DROP TABLE IF EXISTS q2_results;

-- Create table to store results
CREATE EXTERNAL TABLE q2_results (
    offense_description STRING,
    crime_count INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/team30/project/hive/warehouse/q2';

-- To not display table names with column names
SET hive.resultset.use.unique.column.names = false;

-- Insert data from our query
INSERT INTO q2_results
SELECT
    OFNS_DESC as offense_description,
    COUNT(*) as crime_count
FROM nypd_complaints_buck
WHERE OFNS_DESC IS NOT NULL
GROUP BY OFNS_DESC
ORDER BY crime_count DESC;

-- Export results to a CSV file for visualization
INSERT OVERWRITE DIRECTORY '/user/team30/project/output/q2'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM q2_results;

-- Display results
SELECT * FROM q2_results;