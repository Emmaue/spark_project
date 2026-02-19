import os
import requests
import boto3
import json
import time

# --- CONFIGURATION ---
BUCKET_NAME = "avzdax-project-datalake-emmanuel-justice" # <--- VERIFY THIS MATCHES YOUR BUCKET
VIDEO_URL = "https://videos.pexels.com/video-files/2103099/2103099-hd_1920_1080_30fps.mp4"
VIDEO_ID = "traffic_lagos_sample_01"

def upload_to_s3(local_file, s3_key):
    s3 = boto3.client('s3')
    try:
        print(f"☁️ Uploading {local_file} to s3://{BUCKET_NAME}/{s3_key}...")
        s3.upload_file(local_file, BUCKET_NAME, s3_key)
        return True
    except Exception as e:
        print(f"❌ Upload Failed: {e}")
        return False

def ingest_data():
    # 1. Setup Local Paths
    os.makedirs("downloads", exist_ok=True)
    video_path = f"downloads/{VIDEO_ID}.mp4"
    metadata_path = f"downloads/{VIDEO_ID}.json"

    # 2. Download the Video (Using standard Request, no blockers)
    print(f"⬇️ Downloading video from Pexels...")
    start_time = time.time()
    
    with requests.get(VIDEO_URL, stream=True) as r:
        r.raise_for_status()
        with open(video_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    
    print(f"✅ Download complete! ({time.time() - start_time:.2f}s)")

    # 3. Create "Fake" Metadata (To mimic YouTube's data)
    # We do this so your future Spark job has a JSON file to read.
    metadata = {
        "id": VIDEO_ID,
        "title": "Heavy Traffic in Lagos - Lekki Phase 1",
        "uploader": "Pexels Stock",
        "upload_date": "20260218",
        "view_count": 15000,
        "duration": 45,
        "description": "Footage of heavy traffic flow for computer vision analysis."
    }
    
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f)

    # 4. Upload to S3
    upload_to_s3(video_path, f"raw/videos/{VIDEO_ID}.mp4")
    upload_to_s3(metadata_path, f"raw/metadata/{VIDEO_ID}.json")

    # 5. Cleanup
    os.remove(video_path)
    os.remove(metadata_path)
    print("🧹 Local files cleaned up. Pipeline Success!")

if __name__ == "__main__":
    ingest_data()