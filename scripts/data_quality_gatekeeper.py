import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from dotenv import load_dotenv
import os
import boto3

load_dotenv()

def run_data_quality_checks():
    print("🛡️ Starting Data Quality Gatekeeper...")

    # 1. Initialize Spark (Same configuration as your transformation script)
    spark = SparkSession.builder \
        .appName("Data_Quality_Gatekeeper") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .getOrCreate()

    # Use your exact bucket name
    BUCKET_NAME = os.getenv("BUCKET_NAME")
    gold_path = f"s3a://{BUCKET_NAME}/gold/enriched_traffic_summary"

    try:
        print(f"⏳ Reading Gold data from {gold_path}...")
        df = spark.read.parquet(gold_path)
    except Exception as e:
        print(f"❌ CRITICAL ERROR: Could not read Gold data. {e}")
        sys.exit(1) # Crash the script

    # --- 2. DEFINE THE DATA QUALITY RULES ---
    
    # Rule 1: The table must not be empty
    row_count = df.count()
    if row_count == 0:
        print("❌ DQ FAIL: The Gold table has 0 rows. Spark transformation likely failed or wrote empty data.")
        sys.exit(1)
    else:
        print(f"✅ DQ PASS: Table contains {row_count} rows.")

    # Rule 2: 'video_id' cannot be NULL (Primary Key Check)
    null_ids = df.filter(col("video_id").isNull()).count()
    if null_ids > 0:
        print(f"❌ DQ FAIL: Found {null_ids} rows with a NULL video_id.")
        sys.exit(1)
    else:
        print("✅ DQ PASS: No NULL Primary Keys found.")

    # Rule 3: 'total_vehicles_detected' cannot be negative
    negative_vehicles = df.filter(col("total_vehicles_detected") < 0).count()
    if negative_vehicles > 0:
        print(f"❌ DQ FAIL: Found {negative_vehicles} rows with negative vehicle counts. Computer Vision logic is broken.")
        sys.exit(1)
    else:
        print("✅ DQ PASS: All vehicle counts are positive numbers.")

    # --- 3. FINAL VERDICT ---
    print("🏆 ALL DATA QUALITY CHECKS PASSED! The data is safe for the Production Dashboard.")
    spark.stop()

if __name__ == "__main__":
    run_data_quality_checks()