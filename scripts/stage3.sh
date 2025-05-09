#!/bin/bash
set -euo pipefail

# Define directories
HDFS_BASE="/user/team30/project"
OUTPUT_DIR="./output"
MODELS_DIR="./models"
DATA_DIR="./data"

# Check if scripts/model.py exists
if [ ! -f "scripts/new_model.py" ]; then
    echo "Error: scripts/new_model.py not found. Please run this script from the repository root."
    exit 1
fi

echo "=== [1] Setting up directories ==="
mkdir -p "$OUTPUT_DIR"
mkdir -p "$MODELS_DIR"
mkdir -p "$DATA_DIR"
hdfs dfs -mkdir -p "$HDFS_BASE/data"
hdfs dfs -mkdir -p "$HDFS_BASE/models"
hdfs dfs -mkdir -p "$HDFS_BASE/output"

echo "=== [2] Running Spark ML pipeline ==="
spark-submit  --master yarn scripts/new_model.py > "$OUTPUT_DIR/spark_job.log" 2>&1
if [ $? -ne 0 ]; then
    echo "Spark job failed. Check $OUTPUT_DIR/spark_job.log for details."
    exit 1
fi

bash scripts/stage3_results_to_hive.sh

echo "=== [3] Checking output files ==="
ls -l "$OUTPUT_DIR"
ls -l "$MODELS_DIR"
ls -l "$DATA_DIR"

echo "=== [4] Checking HDFS outputs ==="
hdfs dfs -ls "$HDFS_BASE/data"
hdfs dfs -ls "$HDFS_BASE/models"
hdfs dfs -ls "$HDFS_BASE/output"
