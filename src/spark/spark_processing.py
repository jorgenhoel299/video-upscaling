from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, lit
from pyspark.sql.types import BinaryType
import io
from PIL import Image
import torch
import numpy as np
import sys
import os


# Get the current working directory
current_dir = os.getcwd()
print(current_dir)
# Navigate two levels up and then to the 'app' folder
models_directory = os.path.join(current_dir, 'app')

# Get the absolute path
models_directory = os.path.abspath(models_directory)

# Check if the 'models.py' file exists in the directory
models_file_path = os.path.join(models_directory, 'models.py')
if os.path.exists(models_file_path):
    # Append the directory containing models.py to sys.path
    sys.path.append(models_directory)
    print(f"Added {models_directory} to sys.path")
else:
    raise FileNotFoundError(f"{models_file_path} does not exist.")
# Initialize Spark session
spark = SparkSession.builder \
    .appName("FrameUpscalingProcessing") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .getOrCreate()

# Device configuration
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

# Define UDF for processing frames
def process_frame(frame_bytes, srgan_checkpoint_path):
    """Upscale a single frame using the pre-trained GAN model."""
    # Load the model from the checkpoint path
    print("Processing frame...")
    print("Loading model from checkpoint...")
    model_checkpoint = torch.load(srgan_checkpoint_path, map_location=device)
    srgan_generator = model_checkpoint['generator'].to(device)
    print("Model loaded successfully.")
    # Process the frame
    image = Image.open(io.BytesIO(frame_bytes)).convert("RGB")
    input_tensor = torch.from_numpy(np.array(image)).permute(2, 0, 1).float() / 255
    input_tensor = input_tensor.unsqueeze(0)
    
    with torch.no_grad():
        output_tensor = srgan_generator(input_tensor)
    
    output_image = (output_tensor.squeeze(0).permute(1, 2, 0) * 255).byte().numpy()
    result_image = Image.fromarray(output_image)
    buffer = io.BytesIO()
    result_image.save(buffer, format="JPEG")
    return buffer.getvalue()

# Register the UDF
process_frame_udf = udf(process_frame, BinaryType())

# Function to save processed frames to disk
def save_to_directory(batch_df, batch_id):
    """Save processed frames to a directory."""
    output_dir = "/output/processed_frames"  # Directory inside the cluster
    os.makedirs(output_dir, exist_ok=True)
    print(batch_df.columns)
    print(batch_df.value[0])
    print('hi')
    print(batch_df.processed_frame[0])

    # Collect processed frames and save each as a JPEG file
    for idx, row in enumerate(batch_df.collect()):
        frame_id = f"frame_{batch_id}_{idx}.jpg"  # Use index as fallback for id
        frame_path = os.path.join(output_dir, frame_id)
        with open(frame_path, "wb") as f:
            f.write(row["processed_frame"])

# Read frames from Kafka
frames_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "webcam-frames") \
    .load()

query = frames_df.selectExpr("CAST(value AS STRING)") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()
# Process frames
frame_bytes_df = frames_df.select(frames_df.value.cast(BinaryType()).alias("value"))

# Define the checkpoint path as a string literal
srgan_checkpoint_path = "/opt/spark/app/model/checkpoint_srgan.pth.tar"  # Path to your checkpoint

# Use lit() to pass the checkpoint path as a constant to the UDF
processed_frames_df = frame_bytes_df.withColumn(
    "processed_frame", process_frame_udf(frame_bytes_df["value"], lit(srgan_checkpoint_path))
)

# Write processed frames to a directory
query = processed_frames_df.writeStream \
    .foreachBatch(save_to_directory) \
    .outputMode("append") \
    .start()

query.awaitTermination()
