from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import DataStreamReader
import json

# Create Spark Session
spark = SparkSession.builder \
    .appName("VideoUpscaling") \
    .master("spark://spark:7077") \  # Change to appropriate master address if needed
    .getOrCreate()

# Read stream from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "video-frames") \
    .load()

def process_frame(frame_data):
    """Simulate the processing of a video frame (upscaling)."""
    frame = json.loads(frame_data)
    frame_number = frame['frame_number']
    upscaled_data = f'upscaled_{frame["data"]}'  # Placeholder for actual upscaling logic
    print(f"Processed Frame: {frame_number}, Upscaled Data: {upscaled_data}")

# Process each row in the DataFrame
query = df.selectExpr("CAST(value AS STRING)") \
    .writeStream \
    .foreach(process_frame) \
    .start()

query.awaitTermination()