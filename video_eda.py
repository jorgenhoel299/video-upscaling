from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Video Processing") \
    .getOrCreate()

# Reading video files from the directory
video_df = spark.read.format("binaryFile").load("/opt/spark/video_dataset/*.mp4")

# Display the schema of the DataFrame
print("Schema of the video DataFrame:")
video_df.printSchema()

# Show the first few records in the DataFrame
print("First few records in the DataFrame:")
video_df.show(truncate=False)

# Count the total number of video files loaded
total_files = video_df.count()
print(f"Total number of video files: {total_files}")

# Display basic statistics for the video files
print("Basic statistics for the video files:")
video_df.describe().show()

# Display the file names and their lengths (size in bytes)
print("File names and their sizes:")
file_info_df = video_df.select("path", "length")
file_info_df.show(truncate=False)

# Check for null or missing values in the dataset
missing_files_count = video_df.filter(col("path").isNull()).count()
print(f"Number of records with missing file paths: {missing_files_count}")

# If you want to explore further, you can show specific metadata about the videos
# For example, let's show the length of the first 5 video files
print("Length of the first 5 video files (in bytes):")
video_df.select("path", "length").show(5, truncate=False)

# Stop the Spark session when done
spark.stop()
