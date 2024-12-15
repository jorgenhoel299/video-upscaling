# Use Ubuntu as the base image
FROM ubuntu:20.04

# Set Spark home directory
ENV SPARK_HOME=/opt/spark

# Install necessary system packages including wget
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y openjdk-11-jdk curl ffmpeg python3 python3-pip wget git && \
    apt-get clean

# Install librdkafka from source
RUN git clone https://github.com/edenhill/librdkafka.git && \
    cd librdkafka && \
    git checkout v2.6.0 && \
    ./configure && \
    make && \
    make install && \
    cd .. && \
    rm -rf librdkafka

# Install Python libraries including pandas and OpenCV
RUN apt-get update && apt-get install -y \
    python3-opencv libgl1 libglib2.0-0 && \
    python3 -m pip install --upgrade pip && \
    python3 -m pip install pandas && \
    rm -rf /var/lib/apt/lists/

# Install PyTorch and torchvision (CUDA support if needed)
RUN python3 -m pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cpu

# Print environment variables to verify installation
RUN echo "SPARK_HOME: ${SPARK_HOME}" && \
    echo "PATH: ${PATH}"

# Download and install Spark
RUN curl -L -o /tmp/spark.tgz https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt/ && \
    mv /opt/spark-3.5.3-bin-hadoop3 ${SPARK_HOME} && \
    rm /tmp/spark.tgz && \
    ls -la ${SPARK_HOME}  # To check the Spark installation

# Set the working directory to Spark's home
WORKDIR ${SPARK_HOME}

# Add Spark to the PATH
ENV PATH=$PATH:${SPARK_HOME}/bin

# Copy your application code
COPY src /opt/spark/src

# Default command
CMD ["sh", "-c", " \
  if [ \"$SPARK_MODE\" = \"master\" ]; then \
    $SPARK_HOME/sbin/start-master.sh && tail -f /dev/null; \
  elif [ \"$SPARK_MODE\" = \"worker\" ]; then \
    $SPARK_HOME/sbin/start-worker.sh $SPARK_MASTER_URL && tail -f /dev/null; \
  else \
    echo 'Invalid SPARK_MODE. Use master or worker.' && exit 1; \
  fi \
"]
