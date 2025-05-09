-- Q3: Crime trends by month and year
-- This query analyzes the crime trends over time

USE team30_projectdb;

-- Drop the results table if it exists
DROP TABLE IF EXISTS q3_results;

-- Create table to store results
CREATE EXTERNAL TABLE q3_results (
    year INT,
    month INT,
    crime_count INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/team30/project/hive/warehouse/q3';

-- To not display table names with column names
SET hive.resultset.use.unique.column.names = false;

-- Insert data from our query
INSERT INTO q3_results
SELECT
    YEAR(from_unixtime(CAST(CMPLNT_FR_DT/1000 AS BIGINT), 'yyyy-MM-dd HH:mm:ss.SSS')) as year,
    MONTH(from_unixtime(CAST(CMPLNT_FR_DT/1000 AS BIGINT), 'yyyy-MM-dd HH:mm:ss.SSS')) as month,
    COUNT(*) as crime_count
FROM nypd_complaints_part
WHERE CMPLNT_FR_DT IS NOT NULL
GROUP BY YEAR(from_unixtime(CAST(CMPLNT_FR_DT/1000 AS BIGINT), 'yyyy-MM-dd HH:mm:ss.SSS')), MONTH(from_unixtime(CAST(CMPLNT_FR_DT/1000 AS BIGINT), 'yyyy-MM-dd HH:mm:ss.SSS'))
ORDER BY year, month;

-- Export results to a CSV file for visualization
INSERT OVERWRITE DIRECTORY '/user/team30/project/output/q3'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM q3_results;

-- Display results
SELECT * FROM q3_results;