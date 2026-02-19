import cv2
import boto3
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
BUCKET_NAME = os.getenv("BUCKET_NAME")
DOWNLOAD_DIR = "downloads"

def get_unprocessed_videos(s3_client):
    """Compares raw videos with silver parquets to find what needs processing."""
    print("🔍 Checking S3 for unprocessed videos...")
    
    # 1. Get all raw videos
    raw_response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix="raw/videos/")
    raw_videos = [obj['Key'] for obj in raw_response.get('Contents', []) if obj['Key'].endswith('.mp4')]
    
    # 2. Get all processed silver files
    silver_response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix="silver/traffic_counts/")
    silver_files = [obj['Key'] for obj in silver_response.get('Contents', []) if obj['Key'].endswith('.parquet')]
    
    # 3. Find the difference (Idempotency check)
    unprocessed = []
    for raw_vid in raw_videos:
        video_id = raw_vid.split('/')[-1].replace('.mp4', '')
        expected_silver = f"silver/traffic_counts/{video_id}_counts.parquet"
        
        if expected_silver not in silver_files:
            unprocessed.append(raw_vid)
            
    print(f"📊 Found {len(raw_videos)} total videos. {len(unprocessed)} need processing.")
    return unprocessed

def process_video(local_video_path):
    """Uses OpenCV Background Subtraction to detect movement."""
    cap = cv2.VideoCapture(local_video_path)
    fgbg = cv2.createBackgroundSubtractorMOG2(history=500, varThreshold=50, detectShadows=True)
    
    frame_count = 0
    movement_data = []
    
    while True:
        ret, frame = cap.read()
        if not ret: break
        frame_count += 1
        
        # Sample 1 frame per second
        if frame_count % 30 != 0: continue
            
        fgmask = fgbg.apply(frame)
        _, thresh = cv2.threshold(fgmask, 254, 255, cv2.THRESH_BINARY)
        contours, _ = cv2.findContours(thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        
        cars_in_frame = sum(1 for contour in contours if cv2.contourArea(contour) > 500)
                
        movement_data.append({
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "video_second": frame_count // 30,
            "detected_vehicles": cars_in_frame
        })
        
    cap.release()
    return pd.DataFrame(movement_data)

def extract_features():
    s3 = boto3.client('s3')
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    
    # Get the list of videos that haven't been processed yet
    videos_to_process = get_unprocessed_videos(s3)
    
    if not videos_to_process:
        print("✅ All videos are already processed! Exiting.")
        return
        
    for s3_key in videos_to_process:
        video_id = s3_key.split('/')[-1].replace('.mp4', '')
        local_vid = f"{DOWNLOAD_DIR}/{video_id}.mp4"
        local_parquet = f"{DOWNLOAD_DIR}/{video_id}_counts.parquet"
        
        print(f"\n⚙️ Starting processing for: {video_id}")
        
        # Download -> Process -> Save -> Upload -> Cleanup
        s3.download_file(BUCKET_NAME, s3_key, local_vid)
        
        df = process_video(local_vid)
        df.to_parquet(local_parquet, index=False)
        
        silver_key = f"silver/traffic_counts/{video_id}_counts.parquet"
        s3.upload_file(local_parquet, BUCKET_NAME, silver_key)
        
        os.remove(local_vid)
        os.remove(local_parquet)
        print(f"✅ Finished and uploaded to {silver_key}")

    print("\n🧹 Batch Pipeline Complete!")

if __name__ == "__main__":
    extract_features()