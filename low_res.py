import os
import cv2
from pyspark.sql import SparkSession

def discover_frame_directories(base_path="/opt/spark/data/training/frames/"):
    """
    Discover directories containing frames to be downscaled.
    """
    return [
        os.path.join(base_path, directory)
        for directory in os.listdir(base_path)
        if os.path.isdir(os.path.join(base_path, directory))
    ]

def downscale_frame(frame_path, output_dir, scale=0.5):
    """
    Downscale a single frame and save it in the specified output directory.
    """
    frame = cv2.imread(frame_path)
    if frame is None:
        return f"Failed to read frame: {frame_path}"

    # Calculate new dimensions
    height, width = frame.shape[:2]
    new_dimensions = (int(width * scale), int(height * scale))

    # Downscale the frame
    downscaled_frame = cv2.resize(frame, new_dimensions, interpolation=cv2.INTER_AREA)

    # Save the downscaled frame
    frame_name = os.path.basename(frame_path)
    output_path = os.path.join(output_dir, frame_name)
    os.makedirs(output_dir, exist_ok=True)
    cv2.imwrite(output_path, downscaled_frame)

    return f"Downscaled frame saved: {output_path}"

def process_frame_directory(frame_dir, scale=0.5):
    """
    Downscale all frames in a directory and save the results.
    """
    low_res_dir = f"{frame_dir}_lowres"
    log_messages = []

    for frame in os.listdir(frame_dir):
        frame_path = os.path.join(frame_dir, frame)
        if os.path.isfile(frame_path):
            log_messages.append(downscale_frame(frame_path, low_res_dir, scale))

    return log_messages

def process_all_frames(base_path="/opt/spark/data/training/frames/"):
    """
    Discover and process all directories of frames.
    """
    frame_directories = discover_frame_directories(base_path)
    all_logs = []

    for frame_dir in frame_directories:
        print(f"Processing directory: {frame_dir}")
        all_logs.extend(process_frame_directory(frame_dir))

    return all_logs

def run_on_master():
    """
    Ensure the master processes its local frames.
    """
    print("Processing frames on the master container...")
    results = process_all_frames()
    for log in results:
        print(log)

# Initialize Spark
spark = SparkSession.builder.appName("DownscaleFrames").getOrCreate()

# Run processing explicitly on the master node
if "master" in spark.sparkContext.master or spark.sparkContext.master == "local":
    run_on_master()

num_executors = len(spark.sparkContext._jsc.sc().statusTracker().getExecutorInfos())
partitions = max(1, num_executors)  # Ensure at least one partition
# Run processing on worker nodes
results = (
    spark.sparkContext.parallelize([None] * partitions, partitions)
    .mapPartitions(lambda _: process_all_frames())
    .collect()
)

# Print the results from workers
for result in results:
    for log in result:
        print(log)

spark.stop()
