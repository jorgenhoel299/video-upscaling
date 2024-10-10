from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import logging
import subprocess
import json


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
example_video_name = video_df.select("path").collect()[42].path[5:]
print("Path off example video: {0}".format(example_video_name))
metadata = get_video_metadata(example_video_name)
print("Metadata of example video")
print(metadata)
print('______________________________')


# Stop the Spark session when done
spark.stop()