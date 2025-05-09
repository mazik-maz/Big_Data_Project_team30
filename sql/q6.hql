-- Q6: Crime completion rate analysis
-- This query analyzes completed vs attempted crimes

USE team30_projectdb;

-- Drop the results table if it exists
DROP TABLE IF EXISTS q6_results;

-- Create table to store results
CREATE EXTERNAL TABLE q6_results (
    crime_status STRING,
    offense_category STRING,
    crime_count INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/team30/project/hive/warehouse/q6';

-- To not display table names with column names
SET hive.resultset.use.unique.column.names = false;

-- Insert data from our query
INSERT INTO q6_results
SELECT
    CRM_ATPT_CPTD_CD as crime_status,
    OFNS_DESC as offense_category,
    COUNT(*) as crime_count
FROM nypd_complaints_buck
WHERE CRM_ATPT_CPTD_CD IS NOT NULL
  AND CRM_ATPT_CPTD_CD != '(null)'
  AND OFNS_DESC IS NOT NULL
GROUP BY CRM_ATPT_CPTD_CD, OFNS_DESC
ORDER BY crime_count DESC;

-- Export results to a CSV file for visualization
INSERT OVERWRITE DIRECTORY '/user/team30/project/output/q6'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM q6_results;

-- Display results
SELECT * FROM q6_results;