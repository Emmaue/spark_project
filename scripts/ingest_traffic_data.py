import os
import requests
import boto3
import json
from dotenv import load_dotenv

# Load the environment variables from the .env file
load_dotenv()

# --- CONFIGURATION ---
# Safely pull the key using os.getenv
API_KEY = os.getenv("PIXABAY_API_KEY") 
BUCKET_NAME = os.getenv("BUCKET_NAME")
SEARCH_QUERY = "traffic"
BATCH_SIZE = 10 

# ... [The rest of your code remains exactly the same] ... 

def s3_exists(s3_client, key):
    try:
        s3_client.head_object(Bucket=BUCKET_NAME, Key=key)
        return True
    except:
        return False

def ingest_batch():
    s3 = boto3.client('s3')
    # Pixabay Video API endpoint
    url = f"https://pixabay.com/api/videos/?key={API_KEY}&q={SEARCH_QUERY}&per_page=50"
    
    response = requests.get(url).json()
    videos_found = response.get('hits', [])
    
    ingested_count = 0
    os.makedirs("downloads", exist_ok=True)
    
    for v in videos_found:
        if ingested_count >= BATCH_SIZE:
            break
            
        # Use Pixabay's ID to prevent duplicates
        video_id = f"pixabay_{v['id']}"
        s3_key = f"raw/videos/{video_id}.mp4"
        
        # 1. THE CHECK: Idempotency (Skip if already in S3)
        if s3_exists(s3, s3_key):
            continue
            
        # 2. Download (Using the 'medium' size for speed)
        print(f"⬇️ Downloading new video: {video_id}...")
        video_url = v['videos']['medium']['url']
        
        res = requests.get(video_url, stream=True)
        local_path = f"downloads/{video_id}.mp4"
        
        with open(local_path, 'wb') as f:
            for chunk in res.iter_content(chunk_size=8192):
                f.write(chunk)
                
        # 3. Structured Metadata
        metadata = {
            "id": video_id,
            "title": f"Traffic Analysis - {v['tags']}",
            "duration": v['duration'],
            "views": v['views'],
            "ingestion_date": "2026-02-19"
        }
        
        # 4. Multi-Format S3 Upload
        s3.upload_file(local_path, BUCKET_NAME, s3_key)
        s3.put_object(
            Body=json.dumps(metadata), 
            Bucket=BUCKET_NAME, 
            Key=f"raw/metadata/{video_id}.json"
        )
        
        # 5. Cleanup local storage
        os.remove(local_path)
        ingested_count += 1
        print(f"✅ Batch Progress: {ingested_count}/{BATCH_SIZE}")

if __name__ == "__main__":
    ingest_batch()