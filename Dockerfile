# Use Ubuntu as the base image
FROM ubuntu:20.04

# Install necessary packages
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y openjdk-11-jdk curl ffmpeg python3 python3-pip && \
    pip3 install opencv-python && \
    apt-get clean

# Set environment variables for Spark
ENV SPARK_VERSION=3.5.3
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark

# Download and install Spark
RUN curl -L -o /tmp/spark.tgz https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt/ && \
    mv /opt/spark-3.5.3-bin-hadoop3 ${SPARK_HOME} && \
    rm /tmp/spark.tgz

# Set the working directory
WORKDIR ${SPARK_HOME}

# Add Spark to the PATH
ENV PATH=$PATH:${SPARK_HOME}/bin

# Copy your application code
COPY src /opt/spark/src
COPY data/Normal_Videos_for_Event_Recognition /opt/spark/video_dataset

# Default command
CMD ["bash"]
