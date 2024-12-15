import os
import time
from kafka import KafkaProducer

frame_dir = os.getenv("FRAME_DIR", "/frames")
kafka_broker = os.getenv("KAFKA_BROKER", "kafka:9092")
topic = os.getenv("KAFKA_TOPIC", "webcam-frames")
producer = KafkaProducer(bootstrap_servers=kafka_broker, retries=5, acks='all')
for frame_name in sorted(os.listdir(frame_dir)):
    print('bye')
    frame_path = os.path.join(frame_dir, frame_name)
    with open(frame_path, "rb") as frame_file:
        producer.send(topic, frame_file.read())
    print(f"Sent frame: {frame_name}")
    time.sleep(2)  # Mimics real-time streaming

producer.close()
