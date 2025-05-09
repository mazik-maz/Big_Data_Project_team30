DROP DATABASE IF EXISTS team30_projectdb CASCADE;

-- Create an external Hive database (schema) for the project (if not exists)
CREATE DATABASE IF NOT EXISTS team30_projectdb LOCATION '/user/team30/project/hive/warehouse';
USE team30_projectdb;

SHOW TABLES;

-- Create external table for NYPD complaints data stored as Parquet
CREATE EXTERNAL TABLE IF NOT EXISTS nypd_complaints_parquet (
    CMPLNT_NUM         BIGINT,
    CMPLNT_FR_DT       BIGINT,
    CMPLNT_FR_TM       STRING,
    CMPLNT_TO_DT       BIGINT,
    CMPLNT_TO_TM       STRING,
    ADDR_PCT_CD        INT,
    RPT_DT             BIGINT,
    KY_CD              INT,
    OFNS_DESC          STRING,
    PD_CD              INT,
    PD_DESC            STRING,
    CRM_ATPT_CPTD_CD   STRING,
    LAW_CAT_CD         STRING,
    BORO_NM            STRING,
    LOC_OF_OCCUR_DESC  STRING,
    PREM_TYP_DESC      STRING,
    JURIS_DESC         STRING,
    JURISDICTION_CODE  INT,
    PARKS_NM           STRING,
    HADEVELOPT         STRING,
    HOUSING_PSA        STRING,
    X_COORD_CD         INT,
    Y_COORD_CD         INT,
    SUSP_AGE_GROUP     STRING,
    SUSP_RACE          STRING,
    SUSP_SEX           STRING,
    TRANSIT_DISTRICT   INT,
    LATITUDE           DOUBLE PRECISION,
    LONGITUDE          DOUBLE PRECISION,
    Lat_Lon            STRING,
    PATROL_BORO        STRING,
    STATION_NAME       STRING,
    VIC_AGE_GROUP      STRING,
    VIC_RACE           STRING,
    VIC_SEX            STRING
)
STORED AS PARQUET
LOCATION '/user/team30/project/warehouse/nypd_complaints'
TBLPROPERTIES ("parquet.compress"="SNAPPY");

-- For checking tables
SHOW TABLES;

DESCRIBE EXTENDED nypd_complaints_parquet;

SELECT * FROM nypd_complaints_parquet LIMIT 10;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- Create a partitioned table by BORO_NM (borough name) and LAW_CAT_CD (law category code)
CREATE EXTERNAL TABLE IF NOT EXISTS nypd_complaints_part (
    CMPLNT_NUM         BIGINT,
    CMPLNT_FR_DT       BIGINT,
    CMPLNT_FR_TM       STRING,
    CMPLNT_TO_DT       BIGINT,
    CMPLNT_TO_TM       STRING,
    ADDR_PCT_CD        INT,
    RPT_DT             BIGINT,
    KY_CD              INT,
    OFNS_DESC          STRING,
    PD_CD              INT,
    PD_DESC            STRING,
    CRM_ATPT_CPTD_CD   STRING,
    LOC_OF_OCCUR_DESC  STRING,
    PREM_TYP_DESC      STRING,
    JURIS_DESC         STRING,
    JURISDICTION_CODE  INT,
    PARKS_NM           STRING,
    HADEVELOPT         STRING,
    HOUSING_PSA        STRING,
    X_COORD_CD         INT,
    Y_COORD_CD         INT,
    SUSP_AGE_GROUP     STRING,
    SUSP_RACE          STRING,
    SUSP_SEX           STRING,
    TRANSIT_DISTRICT   INT,
    LATITUDE           DOUBLE,
    LONGITUDE          DOUBLE,
    Lat_Lon            STRING,
    PATROL_BORO        STRING,
    STATION_NAME       STRING,
    VIC_AGE_GROUP      STRING,
    VIC_RACE           STRING,
    VIC_SEX            STRING
)
PARTITIONED BY (BORO_NM STRING, LAW_CAT_CD STRING)
STORED AS PARQUET
LOCATION '/user/team30/project/hive/warehouse/nypd_complaints_part'
TBLPROPERTIES ("parquet.compress"="SNAPPY");

-- Create a bucketed table based on complaint type (OFNS_DESC)
CREATE EXTERNAL TABLE IF NOT EXISTS nypd_complaints_buck (
    CMPLNT_NUM         BIGINT,
    CMPLNT_FR_DT       BIGINT,
    CMPLNT_FR_TM       STRING,
    CMPLNT_TO_DT       BIGINT,
    CMPLNT_TO_TM       STRING,
    ADDR_PCT_CD        INT,
    RPT_DT             BIGINT,
    KY_CD              INT,
    PD_CD              INT,
    PD_DESC            STRING,
    CRM_ATPT_CPTD_CD   STRING,
    LAW_CAT_CD         STRING,
    BORO_NM            STRING,
    LOC_OF_OCCUR_DESC  STRING,
    PREM_TYP_DESC      STRING,
    JURIS_DESC         STRING,
    JURISDICTION_CODE  INT,
    PARKS_NM           STRING,
    HADEVELOPT         STRING,
    HOUSING_PSA        STRING,
    X_COORD_CD         INT,
    Y_COORD_CD         INT,
    SUSP_AGE_GROUP     STRING,
    SUSP_RACE          STRING,
    SUSP_SEX           STRING,
    TRANSIT_DISTRICT   INT,
    LATITUDE           DOUBLE,
    LONGITUDE          DOUBLE,
    Lat_Lon            STRING,
    PATROL_BORO        STRING,
    STATION_NAME       STRING,
    VIC_AGE_GROUP      STRING,
    VIC_RACE           STRING,
    VIC_SEX            STRING
)
PARTITIONED BY (OFNS_DESC STRING)
CLUSTERED BY (KY_CD) INTO 10 BUCKETS
STORED AS PARQUET
LOCATION '/user/team30/project/hive/warehouse/nypd_complaints_buck'
TBLPROPERTIES ("parquet.compress"="SNAPPY");

-- Insert data into partitioned table
INSERT OVERWRITE TABLE nypd_complaints_part
PARTITION (BORO_NM, LAW_CAT_CD)
SELECT
    CMPLNT_NUM,
    CMPLNT_FR_DT,
    CMPLNT_FR_TM,
    CMPLNT_TO_DT,
    CMPLNT_TO_TM,
    ADDR_PCT_CD,
    RPT_DT,
    KY_CD,
    OFNS_DESC,
    PD_CD,
    PD_DESC,
    CRM_ATPT_CPTD_CD,
    LOC_OF_OCCUR_DESC,
    PREM_TYP_DESC,
    JURIS_DESC,
    JURISDICTION_CODE,
    PARKS_NM,
    HADEVELOPT,
    HOUSING_PSA,
    X_COORD_CD,
    Y_COORD_CD,
    SUSP_AGE_GROUP,
    SUSP_RACE,
    SUSP_SEX,
    TRANSIT_DISTRICT,
    LATITUDE,
    LONGITUDE,
    Lat_Lon,
    PATROL_BORO,
    STATION_NAME,
    VIC_AGE_GROUP,
    VIC_RACE,
    VIC_SEX,
    BORO_NM,
    LAW_CAT_CD
FROM nypd_complaints_parquet
WHERE BORO_NM IS NOT NULL AND LAW_CAT_CD IS NOT NULL;

-- Insert data into bucketed table
INSERT OVERWRITE TABLE nypd_complaints_buck
PARTITION (OFNS_DESC)
SELECT
    CMPLNT_NUM,
    CMPLNT_FR_DT,
    CMPLNT_FR_TM,
    CMPLNT_TO_DT,
    CMPLNT_TO_TM,
    ADDR_PCT_CD,
    RPT_DT,
    KY_CD,
    PD_CD,
    PD_DESC,
    CRM_ATPT_CPTD_CD,
    LAW_CAT_CD,
    BORO_NM,
    LOC_OF_OCCUR_DESC,
    PREM_TYP_DESC,
    JURIS_DESC,
    JURISDICTION_CODE,
    PARKS_NM,
    HADEVELOPT,
    HOUSING_PSA,
    X_COORD_CD,
    Y_COORD_CD,
    SUSP_AGE_GROUP,
    SUSP_RACE,
    SUSP_SEX,
    TRANSIT_DISTRICT,
    LATITUDE,
    LONGITUDE,
    Lat_Lon,
    PATROL_BORO,
    STATION_NAME,
    VIC_AGE_GROUP,
    VIC_RACE,
    VIC_SEX,
    OFNS_DESC
FROM nypd_complaints_parquet
WHERE OFNS_DESC IS NOT NULL;

-- Verify the data was loaded correctly
SELECT COUNT(*) AS total_complaints FROM nypd_complaints_parquet;
SELECT COUNT(*) AS partitioned_complaints FROM nypd_complaints_part;
SELECT COUNT(*) AS bucketed_complaints FROM nypd_complaints_buck;

-- Display the partitions created
SHOW PARTITIONS nypd_complaints_part;
SHOW PARTITIONS nypd_complaints_buck;

DROP TABLE IF EXISTS nypd_complaints_parquet;