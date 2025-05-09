from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, from_unixtime, to_timestamp, year, month, dayofmonth, hour, sin, cos, lit
from pyspark.ml.feature import StringIndexer, OneHotEncoder, Imputer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier, LinearSVC, OneVsRest, NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
import numpy as np
import os

team = "team30"
warehouse = "project/hive/warehouse"
spark = SparkSession.builder\
    .appName(f"{team} - spark ML")\
    .master("yarn")\
    .config("hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883")\
    .config("spark.sql.warehouse.dir", warehouse)\
    .config("spark.sql.parquet.compression.codec", "snappy")\
    .enableHiveSupport()\
    .getOrCreate()

# Read Hive table
df = spark.read.table('team30_projectdb.nypd_complaints_part')
df = df.orderBy("CMPLNT_FR_DT").limit(1000)

# Define features and target
categorical_cols = ['ADDR_PCT_CD', 'BORO_NM', 'LOC_OF_OCCUR_DESC', 'PREM_TYP_DESC', 'JURIS_DESC', 
                   'JURISDICTION_CODE', 'SUSP_AGE_GROUP', 'SUSP_RACE', 'SUSP_SEX', 
                   'VIC_AGE_GROUP', 'VIC_RACE', 'VIC_SEX', 'CRM_ATPT_CPTD_CD']
numerical_cols = ['LATITUDE', 'LONGITUDE']
target = 'LAW_CAT_CD'

# Preprocess temporal features
df = df.withColumn('CMPLNT_FR_TS', to_timestamp(from_unixtime(col('CMPLNT_FR_DT') / 1000)))
df = df.withColumn('RPT_TS', to_timestamp(from_unixtime(col('RPT_DT') / 1000)))
df = df.withColumn('CMPLNT_year', year('CMPLNT_FR_TS'))
df = df.withColumn('CMPLNT_month', month('CMPLNT_FR_TS'))
df = df.withColumn('CMPLNT_day', dayofmonth('CMPLNT_FR_TS'))
df = df.withColumn('CMPLNT_hour', hour('CMPLNT_FR_TS'))
df = df.withColumn('report_delay', (col('RPT_TS').cast('long') - col('CMPLNT_FR_TS').cast('long')) / 3600)
df = df.withColumn('CMPLNT_month_sin', sin(2 * np.pi * col('CMPLNT_month') / 12))
df = df.withColumn('CMPLNT_month_cos', cos(2 * np.pi * col('CMPLNT_month') / 12))
df = df.withColumn('CMPLNT_day_sin', sin(2 * np.pi * col('CMPLNT_day') / 31))
df = df.withColumn('CMPLNT_day_cos', cos(2 * np.pi * col('CMPLNT_day') / 31))
df = df.withColumn('CMPLNT_hour_sin', sin(2 * np.pi * col('CMPLNT_hour') / 24))
df = df.withColumn('CMPLNT_hour_cos', cos(2 * np.pi * col('CMPLNT_hour') / 24))

# Transform features to nonnegative for Naive Bayes
df = df.withColumn('LATITUDE_shift', col('LATITUDE') + 90)
df = df.withColumn('LONGITUDE_shift', col('LONGITUDE') + 180)
df = df.withColumn('CMPLNT_month_sin_shift', col('CMPLNT_month_sin') + 1)
df = df.withColumn('CMPLNT_month_cos_shift', col('CMPLNT_month_cos') + 1)
df = df.withColumn('CMPLNT_day_sin_shift', col('CMPLNT_day_sin') + 1)
df = df.withColumn('CMPLNT_day_cos_shift', col('CMPLNT_day_cos') + 1)
df = df.withColumn('CMPLNT_hour_sin_shift', col('CMPLNT_hour_sin') + 1)
df = df.withColumn('CMPLNT_hour_cos_shift', col('CMPLNT_hour_cos') + 1)
df = df.withColumn('report_delay', when(col('report_delay') < 0, 0).otherwise(col('report_delay')))

# Update numerical columns
numerical_cols = [
    'LATITUDE_shift', 'LONGITUDE_shift',
    'CMPLNT_year', 'report_delay',
    'CMPLNT_month_sin_shift', 'CMPLNT_month_cos_shift',
    'CMPLNT_day_sin_shift', 'CMPLNT_day_cos_shift',
    'CMPLNT_hour_sin_shift', 'CMPLNT_hour_cos_shift'
]

# Cast integer columns to string for categorical processing
df = df.withColumn('ADDR_PCT_CD', col('ADDR_PCT_CD').cast('string'))
df = df.withColumn('JURISDICTION_CODE', col('JURISDICTION_CODE').cast('string'))

# Handle missing values for categorical columns
df = df.fillna('UNKNOWN', subset=categorical_cols)

# Select relevant columns
selected_cols = numerical_cols + categorical_cols + [target]
df = df.select(selected_cols)

# Build feature extraction pipeline
indexers = [StringIndexer(inputCol=col, outputCol=col + "_index", handleInvalid="keep") for col in categorical_cols]
encoders = [OneHotEncoder(inputCol=indexer.getOutputCol(), outputCol=indexer.getOutputCol() + "_encoded") for indexer in indexers]
imputer = Imputer(inputCols=numerical_cols, outputCols=[col + "_imputed" for col in numerical_cols])
feature_cols_assembled = [encoder.getOutputCol() for encoder in encoders] + [col + "_imputed" for col in numerical_cols]
assembler = VectorAssembler(inputCols=feature_cols_assembled, outputCol="features")
pipeline = Pipeline(stages=indexers + encoders + [imputer, assembler])

# Fit and transform data
model = pipeline.fit(df)
df_transformed = model.transform(df)

df_transformed = df_transformed.select(["features", target])

# Index target variable
target_indexer = StringIndexer(inputCol=target, outputCol="label", handleInvalid="keep")
target_model = target_indexer.fit(df)
df_transformed = target_model.transform(df_transformed)

# Split data
train_data, test_data = df_transformed.randomSplit([0.7, 0.3], seed=42)

# Save train and test datasets
# Save train and test datasets
def run(command):
    return os.popen(command).read()

train_data.select("features", "label").coalesce(1).write.mode("overwrite").format("json").save("project/data/train")
run("hdfs dfs -cat project/data/train/*.json > data/train.json")
test_data.select("features", "label").coalesce(1).write.mode("overwrite").format("json").save("project/data/test")
run("hdfs dfs -cat project/data/test/*.json > data/test.json")
print("______________FEATURES______________")
print(train_data.select("features").take(10))
# Define evaluators
evaluator_accuracy = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
evaluator_f1 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")

# Model 1: Random Forest Classifier
print("_________________1__________________")
rf = RandomForestClassifier(labelCol="label", featuresCol="features")
print("_________________2__________________")
paramGrid_rf = ParamGridBuilder()\
    .addGrid(rf.numTrees, [10, 20, 30])\
    .addGrid(rf.maxDepth, [5, 10, 15])\
    .addGrid(rf.maxBins, [32, 64, 128])\
    .build()
print("_________________3__________________")
cv_rf = CrossValidator(estimator=rf, estimatorParamMaps=paramGrid_rf, evaluator=evaluator_f1, numFolds=3)
print("_________________4__________________")
cv_model_rf = cv_rf.fit(train_data)
print("_________________5__________________")
best_rf_model = cv_model_rf.bestModel
print("_________________6__________________")
predictions_rf = best_rf_model.transform(test_data)
print("_________________7__________________")
accuracy_rf = evaluator_accuracy.evaluate(predictions_rf)
print("_________________8__________________")
f1_rf = evaluator_f1.evaluate(predictions_rf)
print("_________________9__________________")
best_rf_model.write().overwrite().save("project/models/model_rf")
print("_________________10__________________")
run("hdfs dfs -get project/models/model_rf models/model_rf")
predictions_rf.select("label", "prediction").coalesce(1).write.mode("overwrite").format("csv").option("sep", ",").option("header", "true").save("project/output/model_rf_predictions.csv")


def save_hdfs_to_local(hdfs_path, local_path):
    import subprocess
    # Get list of files
    files = subprocess.check_output(f"hdfs dfs -ls {hdfs_path} | awk '{{print $8}}'", shell=True).decode().split()
    # Process files one by one
    with open(local_path, 'w') as outfile:
        for file in files:
            if file.endswith('.csv') or file.endswith('.json'):
                subprocess.run(f"hdfs dfs -cat {file} >> {local_path}", shell=True)

save_hdfs_to_local("project/output/model_rf_predictions.csv/*.csv", "output/model_rf_predictions_output.csv")

# Model 2: One-vs-Rest with Linear SVC
lsvc = LinearSVC(maxIter=10)
print("_________________11__________________")
ovr = OneVsRest(classifier=lsvc, labelCol="label", featuresCol="features")
print("_________________12__________________")
paramGrid_ovr = ParamGridBuilder()\
    .addGrid(lsvc.regParam, [0.001, 0.01, 0.1])\
    .addGrid(lsvc.maxIter, [10, 50, 100])\
    .addGrid(lsvc.tol, [1e-6, 1e-5, 1e-4])\
    .build()
print("_________________13__________________")
cv_ovr = CrossValidator(estimator=ovr, estimatorParamMaps=paramGrid_ovr, evaluator=evaluator_f1, numFolds=3)
print("_________________14__________________")
cv_model_ovr = cv_ovr.fit(train_data)
print("_________________15__________________")
best_ovr_model = cv_model_ovr.bestModel
print("_________________16__________________")
predictions_ovr = best_ovr_model.transform(test_data)
print("_________________17__________________")
accuracy_ovr = evaluator_accuracy.evaluate(predictions_ovr)
print("_________________18__________________")
f1_ovr = evaluator_f1.evaluate(predictions_ovr)
print("_________________19__________________")
best_ovr_model.write().overwrite().save("project/models/model_ovr")
print("_________________20__________________")
run("hdfs dfs -get project/models/model_ovr models/model_ovr")
print("_________________21__________________")
predictions_ovr.select("label", "prediction").coalesce(1).write.mode("overwrite").format("csv").option("sep", ",").option("header", "true").save("project/output/model_ovr_predictions.csv")
save_hdfs_to_local("project/output/model_ovr_predictions.csv/*.csv", "output/model_ovr_predictions_output.csv")
print("_________________22__________________")

# Model 3: Naive Bayes
nb = NaiveBayes(labelCol="label", featuresCol="features")
paramGrid_nb = ParamGridBuilder()\
    .addGrid(nb.smoothing, [0.5, 1.0, 1.5])\
    .build()
cv_nb = CrossValidator(estimator=nb, estimatorParamMaps=paramGrid_nb, evaluator=evaluator_f1, numFolds=3)
cv_model_nb = cv_nb.fit(train_data)
best_nb_model = cv_model_nb.bestModel
predictions_nb = best_nb_model.transform(test_data)
accuracy_nb = evaluator_accuracy.evaluate(predictions_nb)
f1_nb = evaluator_f1.evaluate(predictions_nb)
best_nb_model.write().overwrite().save("project/models/model_nb")
run("hdfs dfs -get project/models/model_nb models/model_nb")
predictions_nb.select("label", "prediction").coalesce(1).write.mode("overwrite").format("csv").option("sep", ",").option("header", "true").save("project/output/model_nb_predictions.csv")
save_hdfs_to_local("project/output/model_nb_predictions.csv/*.csv", "output/model_nb_predictions_output.csv")


# Predict specific data sample
sample = test_data.select("features", "label").limit(5)
sample_predictions_rf = best_rf_model.transform(sample)
sample_predictions_rf.select("label", "prediction").show()

# Compare models
models = [
    ["RandomForestClassifier", accuracy_rf, f1_rf],
    ["OneVsRest_LinearSVC", accuracy_ovr, f1_ovr],
    ["NaiveBayes", accuracy_nb, f1_nb]
]
df_eval = spark.createDataFrame(models, ["model", "accuracy", "f1"])
df_eval.show(truncate=False)
df_eval.coalesce(1).write.mode("overwrite").format("csv").option("sep", ",").option("header", "true").save("project/output/evaluation.csv")
run("hdfs dfs -cat project/output/evaluation.csv/*.csv > output/evaluation.csv")

# Stop Spark session
spark.stop()