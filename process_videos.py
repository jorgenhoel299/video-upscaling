import os
import cv2
from pyspark.sql import SparkSession

def discover_local_videos(base_path="/opt/spark/data/training/"):
    """
    Discover all video files in the local base_path.
    """
    return [
        os.path.join(root, file)
        for root, _, files in os.walk(base_path)
        for file in files if file.endswith(".mp4")
    ]

def extract_frames(video_path):
    """
    Extract frames from a single video file.
    """
    output_dir = f"/opt/spark/data/training/frames/{os.path.basename(video_path).split('.')[0]}"
    log_messages = []  # Store logs to return

    # Try to create the output directory
    try:
        os.makedirs(output_dir, exist_ok=True)
        log_messages.append(f"Directory created successfully: {output_dir}")
    except Exception as e:
        log_messages.append(f"Failed to create directory {output_dir}. Error: {str(e)}")
        return "\n".join(log_messages)

    # Open the video file
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        log_messages.append(f"Failed to open video: {video_path}")
        return "\n".join(log_messages)

    log_messages.append(f"Video opened successfully: {video_path}")

    # Initialize frame count and process frames
    count = 0
    success, frame = cap.read()
    log_messages.append(f"Initial frame read status: {success}")

    while success:
        if frame is None:
            log_messages.append(f"Frame {count} is None. Stopping...")
            break

        frame_path = os.path.join(output_dir, f"frame_{count:04d}.jpg")
        result = cv2.imwrite(frame_path, frame)
        if not result:
            log_messages.append(f"Failed to save frame {count} to {frame_path}")
        else:
            log_messages.append(f"Saved frame {count} to {frame_path}")

        count += 1
        success, frame = cap.read()
        log_messages.append(f"Next frame read status: {success}")

    cap.release()
    log_messages.append(f"Processed {count} frames from {video_path}")
    return "\n".join(log_messages)



def process_local_videos(partition):
    """
    Process all videos in a given partition.
    """
    local_videos = discover_local_videos()  # Discover videos local to each node
    results = [extract_frames(video) for video in local_videos]
    return results

# Initialize Spark
spark = SparkSession.builder.appName("ProcessDistributedVideos").getOrCreate()

# Create an RDD with empty partitions to parallelize processing
rdd = spark.sparkContext.parallelize([None] * spark.sparkContext.defaultParallelism)

# Use mapPartitions to process videos on each node
results = rdd.mapPartitions(process_local_videos).collect()

# Print the results
for result in results:
    print(result)

spark.stop()
