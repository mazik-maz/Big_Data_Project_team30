CREATE EXTERNAL TABLE IF NOT EXISTS team30_projectdb.nypd_schema_raw (
  column_name STRING,
  data_type   STRING,
  comment     STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '/user/team30/project/hive/schema_raw/';

CREATE OR REPLACE VIEW team30_projectdb.nypd_schema AS
SELECT
  column_name,
  data_type
FROM team30_projectdb.nypd_schema_raw
WHERE
  column_name IS NOT NULL
  AND column_name <> ''
  AND column_name NOT LIKE '#%';