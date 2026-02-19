import os
from pyspark.sql import SparkSession

def run_spark_pipeline():
    print("🚀 Initializing Spark Session...")
    # Running in fast Local Mode for Docker
    spark = SparkSession.builder \
        .appName("Traffic_Gold_Pipeline") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .getOrCreate()

    # Optimize for a small cluster
    spark.conf.set("spark.sql.shuffle.partitions", "4")

    # Use your bucket name
    bucket = "avzdax-project-datalake-emmanuel-justice"

    # --- 1. READ SILVER TRAFFIC DATA ---
    print("⏳ Reading Silver Traffic Data...")
    s3_silver_path = f"s3a://{bucket}/silver/simulated_traffic/*.parquet"
    df_traffic = spark.read.parquet(s3_silver_path)
    df_traffic.createOrReplaceTempView("traffic_data")

    # --- 2. READ RAW METADATA ---
    print("⏳ Reading Raw Metadata...")
    s3_meta_path = f"s3a://{bucket}/raw/metadata/*.json"
    df_meta = spark.read.json(s3_meta_path)
    df_meta.createOrReplaceTempView("video_metadata")

    # --- 3. TRANSFORM & JOIN ---
    print("⚙️ Executing Data Transformations & Joins...")
    
    # First, aggregate the 10M rows
    spark.sql("""
        SELECT 
            video_id,
            DATE(timestamp) as traffic_date,
            SUM(detected_vehicles) as total_vehicles_detected,
            ROUND(AVG(detected_vehicles), 2) as avg_vehicles_per_second
        FROM traffic_data
        GROUP BY video_id, DATE(timestamp)
    """).createOrReplaceTempView("daily_traffic")

    # Second, join with metadata
    enriched_gold_df = spark.sql("""
        SELECT 
            t.video_id,
            m.title,
            m.duration as video_duration_seconds,
            m.views as total_pixabay_views,
            t.traffic_date,
            t.total_vehicles_detected,
            t.avg_vehicles_per_second
        FROM daily_traffic t
        LEFT JOIN video_metadata m 
        ON t.video_id = m.id
    """)

    # --- 4. SAVE TO GOLD ---
    gold_path = f"s3a://{bucket}/gold/enriched_traffic_summary"
    print(f"☁️ Saving Enriched Gold data to {gold_path}...")
    
    # Mode overwrite makes it safely idempotent!
    enriched_gold_df.coalesce(1).write.mode("overwrite").parquet(gold_path)

    print("✅ Pipeline Complete! Gold layer is updated.")
    spark.stop()

if __name__ == "__main__":
    run_spark_pipeline()