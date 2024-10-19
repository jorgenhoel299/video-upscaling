# **Video Upscaling Project Using Spark, Kafka, and GAN**

## Overview
This project aims to enhance video resolution through upscaling techniques utilizing Generative Adversarial Networks (GANs). We leverage Apache Spark for distributed processing and Apache Kafka for real-time data streaming to efficiently handle large video datasets. 

## Objectives
- Implement a GAN model for video upscaling.
- Use Spark for distributed processing of video frames to improve scalability and performance.
- Employ Kafka for real-time data streaming, enabling efficient data handling and processing.

## Features
- **Video Frame Extraction**: Efficiently extract frames from input videos for processing.
- **Distributed Processing**: Utilize Spark to parallelize the upscaling process, significantly speeding up computation times.
- **Real-time Data Streaming**: Implement Kafka to stream video data, facilitating continuous processing.
- **Model Training and Evaluation**: Train GAN models on upscaled video data and evaluate their performance against baseline models.

## Technologies Used
- **Apache Spark**: For distributed data processing.
- **Apache Kafka**: For real-time data streaming.
- **Python**: As the primary programming language for model implementation.
- **TensorFlow/PyTorch**: For building and training the GAN models.
