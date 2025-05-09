USE team30_projectdb;

DROP TABLE IF EXISTS model_rf_predictions;
CREATE EXTERNAL TABLE IF NOT EXISTS model_rf_predictions (
  label      DOUBLE,
  prediction DOUBLE
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/team30/project/output/model_rf_predictions.csv'
TBLPROPERTIES (
  "skip.header.line.count"="1"
);

DROP TABLE IF EXISTS model_ovr_predictions;
CREATE EXTERNAL TABLE IF NOT EXISTS model_ovr_predictions (
  label      DOUBLE,
  prediction DOUBLE
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/team30/project/output/model_ovr_predictions.csv'
TBLPROPERTIES (
  "skip.header.line.count"="1"
);

DROP TABLE IF EXISTS model_nb_predictions;
CREATE EXTERNAL TABLE IF NOT EXISTS model_nb_predictions (
  label      DOUBLE,
  prediction DOUBLE
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/team30/project/output/model_nb_predictions.csv'
TBLPROPERTIES (
  "skip.header.line.count"="1"
);

DROP TABLE IF EXISTS evaluation_metrics;
CREATE EXTERNAL TABLE IF NOT EXISTS evaluation_metrics (
  model    STRING,
  accuracy DOUBLE,
  f1       DOUBLE
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/team30/project/output/evaluation.csv'
TBLPROPERTIES (
  "skip.header.line.count"="1"
);



