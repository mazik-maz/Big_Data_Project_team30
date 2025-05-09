DROP TABLE IF EXISTS nypd_complaints CASCADE;

CREATE TABLE IF NOT EXISTS nypd_complaints (
    CMPLNT_NUM         BIGINT PRIMARY KEY,
    CMPLNT_FR_DT       DATE,
    CMPLNT_FR_TM       TIME,
    CMPLNT_TO_DT       DATE,
    CMPLNT_TO_TM       TIME,
    ADDR_PCT_CD        INT,
    RPT_DT             DATE,
    KY_CD              INT,
    OFNS_DESC          TEXT,
    PD_CD              INT,
    PD_DESC            TEXT,
    CRM_ATPT_CPTD_CD   TEXT,
    LAW_CAT_CD         TEXT,
    BORO_NM            TEXT,
    LOC_OF_OCCUR_DESC  TEXT,
    PREM_TYP_DESC      TEXT,
    JURIS_DESC         TEXT,
    JURISDICTION_CODE  INT,
    PARKS_NM           TEXT,
    HADEVELOPT         TEXT,
    HOUSING_PSA        TEXT,
    X_COORD_CD         INT,
    Y_COORD_CD         INT,
    SUSP_AGE_GROUP     TEXT,
    SUSP_RACE          TEXT,
    SUSP_SEX           TEXT,
    TRANSIT_DISTRICT   INT,
    LATITUDE           DOUBLE PRECISION,
    LONGITUDE          DOUBLE PRECISION,
    Lat_Lon            TEXT,
    PATROL_BORO        TEXT,
    STATION_NAME       TEXT,
    VIC_AGE_GROUP      TEXT,
    VIC_RACE           TEXT,
    VIC_SEX            TEXT
);

ALTER TABLE nypd_complaints ADD COLUMN IF NOT EXISTS new_georeferenced_column TEXT;

TRUNCATE TABLE nypd_complaints;