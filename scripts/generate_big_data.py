import pandas as pd
import numpy as np
import boto3
import os
from dotenv import load_dotenv

load_dotenv()
BUCKET_NAME = os.getenv("BUCKET_NAME")

def s3_file_exists(s3_client, key):
    """Checks if a file already exists in S3 to prevent duplicate work."""
    try:
        s3_client.head_object(Bucket=BUCKET_NAME, Key=key)
        return True
    except:
        return False

def generate_10m_rows():
    s3 = boto3.client('s3')
    s3_key = "silver/simulated_traffic/historical_10m_counts.parquet"
    
    # 1. THE IDEMPOTENCY CHECK
    print("🔍 Checking S3 for existing historical data...")
    if s3_file_exists(s3, s3_key):
        print("⏭️ Skipping generation! 10M row dataset is already securely in S3.")
        return

    # 2. Generate Data (Only runs if the file is missing)
    num_rows = 10_000_000
    print(f"🚀 Generating {num_rows} rows of simulated CV traffic data...")
    
    video_ids = [f"pixabay_{i}" for i in range(10000, 10050)]
    
    print("⏳ Building massive DataFrame in memory...")
    data = {
        'video_id': np.random.choice(video_ids, num_rows),
        'timestamp': pd.date_range(end=pd.Timestamp.now(), periods=num_rows, freq='s').astype(str),
        'video_second': np.random.randint(1, 60, size=num_rows),
        'detected_vehicles': np.random.randint(0, 15, size=num_rows)
    }
    
    df = pd.DataFrame(data)
    
    # 3. Save as Parquet
    os.makedirs("downloads", exist_ok=True)
    file_name = "downloads/historical_10m_counts.parquet"
    print(f"💾 Compressing 10M rows to {file_name}...")
    df.to_parquet(file_name, index=False)
    
    # 4. Upload to S3
    print(f"☁️ Uploading to s3://{BUCKET_NAME}/{s3_key}...")
    s3.upload_file(file_name, BUCKET_NAME, s3_key)
    
    # 5. Cleanup
    os.remove(file_name)
    print("✅ Upload complete and local disk cleaned up!")

if __name__ == "__main__":
    generate_10m_rows()