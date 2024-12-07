from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import os

# Step 1: Discover Local Frames
def discover_local_frames(base_path="/opt/spark/data/training/frames/"):
    """
    Discover all frame files in the local container and return their metadata.
    """
    frame_files = [
        (os.path.join(root, file), os.path.basename(root))  # (file_path, video_name)
        for root, _, files in os.walk(base_path)
        for file in files if file.endswith(".jpg")
    ]
    return frame_files

# Step 2: Load Frames into Spark
def load_frames_into_spark(spark, base_path="/opt/spark/data/training/frames/"):
    """
    Load local frames into a Spark DataFrame.
    """
    local_frames = discover_local_frames(base_path)

    schema = StructType([
        StructField("frame_path", StringType(), True),
        StructField("video_name", StringType(), True)
    ])

    local_df = spark.createDataFrame(local_frames, schema)
    return local_df

# Step 3: Repartition the Data
def repartition_frames(frames_df, num_partitions=4):
    """
    Repartition the DataFrame for efficient distribution.
    """
    repartitioned_df = frames_df.repartition(num_partitions, "video_name")
    repartitioned_df.cache()  # Persist for faster operations
    return repartitioned_df

# Step 4: Explore the Dataset
def explore_dataset(repartitioned_df):
    """
    Perform basic exploration on the dataset.
    """
    # Count frames per video
    print("Count of frames per video:")
    frames_count = repartitioned_df.groupBy("video_name").count()
    frames_count.show(truncate=False)

    # Show sample data
    print("Sample frame paths:")
    repartitioned_df.show(10, truncate=False)

    # Check partition distribution
    partition_sizes = repartitioned_df.rdd.glom().map(len).collect()
    print(f"Partition sizes: {partition_sizes}")

# Main Workflow
if __name__ == "__main__":
    # Initialize Spark
    spark = SparkSession.builder.appName("RepartitionAndExploreFrames").getOrCreate()

    # Load frames into Spark
    print("Loading frames into Spark...")
    frames_df = load_frames_into_spark(spark)

    # Repartition the DataFrame
    print("Repartitioning the DataFrame...")
    repartitioned_df = repartition_frames(frames_df, num_partitions=4)

    # Explore the dataset
    print("Exploring the dataset...")
    explore_dataset(repartitioned_df)

    # Stop Spark
    spark.stop()
