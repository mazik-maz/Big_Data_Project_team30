#!/bin/bash

set -euo pipefail

GDRIVE_FILE_ID="1PRXKtP0mDy85utmNA-SqkZFMmEvROrtu"

DATA_DIR="$HOME/data"
RAW_ZIP="$DATA_DIR/nypd_complaint_data.zip"
RAW_CSV="$DATA_DIR/NYPD_Complaint_Data_Historic.csv"


PG_HOST="hadoop-04.uni.innopolis.ru"
PG_PORT="5432"
PG_DB="team30_projectdb"
PG_USER="team30"
PG_PASSWORD="hktzN5Hxb5EYwxCW"
PG_TABLE="nypd_complaints"

HDFS_DIR="/user/team30/project/warehouse"

echo "=== [1] Ensure dataset directory exists ================================="
mkdir -p "$DATA_DIR"

echo "=== [2] Download dataset from Google Drive (if missing) ================="
if [[ ! -f "$RAW_CSV" ]]; then
  echo "CSV not present locally – downloading via gdown…"
  if ! command -v gdown &> /dev/null; then
    echo "  -> gdown not found; installing with pip"
    python3 -m pip install --quiet gdown
    export PATH="$HOME/.local/bin:$PATH"
  fi

  gdown --id "$GDRIVE_FILE_ID" --output "$RAW_ZIP"

  echo "  -> Unzipping dataset…"
  unzip -o "$RAW_ZIP" -d "$DATA_DIR"
  rm -f "$RAW_ZIP"
else
  echo "Local CSV already present – skipping download."
fi

echo "=== [3] Prepare data for copying to PostgreSQL ================="
sed -e 's/(null)//g' data/NYPD_Complaint_Data_Historic.csv > data/nypd_complaints_clean.csv
head -n 7000000 data/nypd_complaints_clean.csv > data/nypd_complaints_clean_2.csv
grep -E '^[0-9]+,' data/nypd_complaints_clean_2.csv > data/nypd_complaints_final.csv

rm data/NYPD_Complaint_Data_Historic.csv data/nypd_complaints_clean.csv data/nypd_complaints_clean_2.csv

echo "=== [4] Copying data to PostgreSQL ================="
python scripts/build_projectdb.py

echo "=== [5] Sqoop import → HDFS: $HDFS_DIR"
hdfs dfs -rm -r -f "$HDFS_DIR" || true
sqoop import \
  --connect "jdbc:postgresql://$PG_HOST/$PG_DB" \
  --username "$PG_USER" --password "$PG_PASSWORD" \
  --table "$PG_TABLE" \
  --warehouse-dir "$HDFS_DIR" \
  --compression-codec=snappy \
  --compress --as-parquetfile \
  --m 1
