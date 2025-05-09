#!/bin/bash
set -euo pipefail

# Directory structure
HDFS_BASE="/user/team30/project"
SQL_DIR="./sql"
OUTPUT_DIR="./output"

echo "=== [1] Setting up directories ==="
# Create local directories if they don't exist
mkdir -p "$OUTPUT_DIR"
mkdir -p "${OUTPUT_DIR}/charts"

# Create HDFS directories
echo "Creating HDFS directories..."
hdfs dfs -mkdir -p "$HDFS_BASE/hive/warehouse"
hdfs dfs -mkdir -p "$HDFS_BASE/output"

# Read Hive password from password file
echo "Reading password from secrets file..."
HIVE_PASSWORD='hktzN5Hxb5EYwxCW'

echo "=== [2] Creating and populating Hive database ==="
# Run the db.hql script to create the Hive database and tables
echo "Creating Hive database and tables..."
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team30 -p "$HIVE_PASSWORD" -f sql/db.hql > "${OUTPUT_DIR}/hive_db_results.txt"

echo "=== [3] Prepare table schema ==="
bash scripts/create_table_schema.sh

echo "=== [4] Running EDA queries ==="
# Array of query files
queries=("q1.hql" "q2.hql" "q3.hql" "q4.hql" "q5.hql" "q6.hql")

# Run each query and save results
for query in "${queries[@]}"; do
    echo "Running query ${query}..."
    beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team30 -p "$HIVE_PASSWORD" -f "${SQL_DIR}/${query}" > "${OUTPUT_DIR}/${query%.hql}_results.txt"

    # Extract the query number
    query_num="${query%.hql}"

    # Create CSV header based on query number
    case "$query_num" in
        q1)
            echo "borough,law_category,crime_count" > "${OUTPUT_DIR}/${query_num}.csv"
            ;;
        q2)
            echo "offense_description,crime_count" > "${OUTPUT_DIR}/${query_num}.csv"
            ;;
        q3)
            echo "year,month,crime_count" > "${OUTPUT_DIR}/${query_num}.csv"
            ;;
        q4)
            echo "premise_type,crime_count" > "${OUTPUT_DIR}/${query_num}.csv"
            ;;
        q5)
            echo "vic_age_group,vic_race,vic_sex,crime_count" > "${OUTPUT_DIR}/${query_num}.csv"
            ;;
        q6)
            echo "crime_status,offense_category,crime_count" > "${OUTPUT_DIR}/${query_num}.csv"
            ;;
    esac

    # Get the results from HDFS and append to the CSV file
    hdfs dfs -cat ${HDFS_BASE}/output/${query_num}/* >> "${OUTPUT_DIR}/${query_num}.csv"
    echo "Results saved to ${OUTPUT_DIR}/${query_num}.csv"
done
