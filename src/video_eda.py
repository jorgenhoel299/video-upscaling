from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import logging
import subprocess
import json
import cv2
import os
import numpy as np

logging.getLogger("org").setLevel("ERROR")
logging.getLogger("akka").setLevel("ERROR")

# Initialize Spark session with increased memory settings
spark = SparkSession.builder \
    .appName("Video Processing") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Reading video files from the directory (modify this path as necessary)
video_df = spark.read.format("binaryFile").load("/opt/spark/video_dataset/*.mp4")

# Display the schema of the DataFrame
print("Schema of the video DataFrame:")
video_df.printSchema()

# Count the total number of video files loaded
total_files = video_df.count()
print(f"Total number of video files: {total_files}")

# Show the first few records in the DataFrame (limited to metadata)
print("First few records in the DataFrame (showing path and length):")
video_df.select("path", "length").show(5, truncate=False)

# Display basic statistics for the video files
print("Basic statistics for the video files:")
video_df.describe("length").show()

# Display the file names and their lengths (size in bytes)
print("File names and their sizes:")
file_info_df = video_df.select("path", "length")
file_info_df.show(truncate=False)

# Check for null or missing values in the dataset
missing_files_count = video_df.filter(col("path").isNull()).count()
print(f"Number of records with missing file paths: {missing_files_count}")

def get_video_metadata(file_path):
    command = [
        "ffprobe",
        "-v", "error",
        "-show_entries", "format=duration,bit_rate",
        "-show_entries", "stream=width,height,r_frame_rate",
        "-of", "json",
        file_path
    ]
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return json.loads(result.stdout)

# Example usage:
example_video_path = video_df.select("path").collect()[42].path[5:]
print("Path off example video: {0}".format(example_video_path))
metadata = get_video_metadata(example_video_path)
print("Metadata of example video")
print(metadata)
print('______________________________')

# VIsual content detection
net = cv2.dnn.readNet("/opt/spark/src/yolov3.weights", "/opt/spark/src/yolov3.cfg")
output_dir = "/opt/spark/src/outputs"
os.makedirs(output_dir, exist_ok=True)

# Load YOLO model
net = cv2.dnn.readNet("/opt/spark/src/yolov3.weights", "/opt/spark/src/yolov3.cfg")

# Initialize video capture from the video file
video_capture = cv2.VideoCapture(example_video_path)

# Get video dimensions
width = int(video_capture.get(cv2.CAP_PROP_FRAME_WIDTH))
height = int(video_capture.get(cv2.CAP_PROP_FRAME_HEIGHT))

def detect_objects(frame, frame_id):
    blob = cv2.dnn.blobFromImage(frame, 1/255.0, (416, 416), swapRB=True, crop=False)
    net.setInput(blob)
    layer_outputs = net.forward(net.getUnconnectedOutLayersNames())
    print("Layer Outputs:", layer_outputs)
    detections = []
    for output in layer_outputs:
        for detection in output:
            scores = detection[5:]  # Assuming first 5 are x, y, w, h, confidence
            class_id = np.argmax(scores)
            confidence = scores[class_id]
            if confidence > 0.5:  # Confidence threshold
                # Calculate bounding box coordinates
                box = detection[0:4] * np.array([width, height, width, height])
                (centerX, centerY, w, h) = box.astype("int")
                x1 = int(centerX - (w / 2))
                y1 = int(centerY - (h / 2))
                detections.append({"box": (x1, y1, x1 + w, y1 + h), "class_id": class_id, "confidence": confidence})
                frame_copy = frame.copy()
                
                # Save the frame with detections
                detected_frame_path = os.path.join(output_dir, f"detected_frame_{frame_id}.jpg")
                frame_copy = frame.copy()
                cv2.rectangle(frame_copy, (x1, y1), (x1 + w, y1 + h), (0, 255, 0), 2)
                cv2.imwrite(detected_frame_path, frame_copy)
                # cv2.imwrite(detected_frame_path, frame)
    
    return detections

frame_id = 0
while True:
    # Read frame from video source
    ret, frame = video_capture.read()
    if not ret:
        break
    if frame_id == 10:
        break #change to see more frames
    # Process frame
    detections = detect_objects(frame, frame_id)
    
    # Increment frame ID
    frame_id += 1

# Release video capture when done
video_capture.release()

# Print summary of detections (optional)
print(f"Processed {frame_id} frames and saved detected frames in {output_dir}.")
# Stop the Spark session when done
spark.stop()