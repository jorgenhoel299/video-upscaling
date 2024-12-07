import os
import shutil
import cv2
from pyspark.sql import SparkSession

def discover_local_videos(base_path="/opt/spark/data/training/"):
    """
    Discover all video files in the local base_path of the container.
    """
    return [
        os.path.join(root, file)
        for root, _, files in os.walk(base_path)
        for file in files if file.endswith(".mp4")
    ]

def remove_existing_frames(base_path="/opt/spark/data/training/frames/"):
    """
    Remove all existing frames and directories where frames are stored.
    """
    if os.path.exists(base_path):
        try:
            shutil.rmtree(base_path)
            os.makedirs(base_path, exist_ok=True)
            return f"Removed existing frames and directories: {base_path}"
        except Exception as e:
            return f"Failed to remove frames directory {base_path}. Error: {str(e)}"
    else:
        os.makedirs(base_path, exist_ok=True)
        return f"Frames directory did not exist, created new: {base_path}"

def extract_frames(video_path):
    """
    Extract frames from a single video file and store them locally.
    """
    output_dir = f"/opt/spark/data/training/frames/{os.path.basename(video_path).split('.')[0]}"
    os.makedirs(output_dir, exist_ok=True)

    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        return f"Failed to open video: {video_path}"

    count = 0
    success, frame = cap.read()
    while success:
        if frame is None:
            break

        frame_path = os.path.join(output_dir, f"frame_{count:04d}.jpg")
        cv2.imwrite(frame_path, frame)
        count += 1
        success, frame = cap.read()

    cap.release()
    return f"Processed {count} frames from {video_path}"

def process_local_videos():
    """
    Discover and process videos in the local container.
    """
    log_messages = []

    # Step 1: Remove existing frames
    log_messages.append(remove_existing_frames())

    # Step 2: Discover local videos
    local_videos = discover_local_videos()
    log_messages.append(f"Discovered videos: {local_videos}")

    # Step 3: Process each video and extract frames
    results = [extract_frames(video) for video in local_videos]
    log_messages.extend(results)

    return log_messages

def run_on_master():
    """
    Ensure the master processes its local videos.
    """
    print("Processing videos on the master container...")
    results = process_local_videos()
    for log in results:
        print(log)

# Initialize Spark
spark = SparkSession.builder.appName("ProcessLocalVideos").getOrCreate()

# Run processing explicitly on the master node
if "master" in spark.sparkContext.master or spark.sparkContext.master == "local":
    run_on_master()

# Run processing on worker nodes
results = spark.sparkContext.parallelize([None], 1).mapPartitions(lambda _: process_local_videos()).collect()

# Print the results from workers
for result in results:
    for log in result:
        print(log)

spark.stop()
