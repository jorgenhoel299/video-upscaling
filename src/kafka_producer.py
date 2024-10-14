import time
import json
from confluent_kafka import Producer

# Kafka Configuration
KAFKA_BROKER = 'kafka:9092'  # Use Kafka service name in Docker

# Create a Kafka Producer
producer = Producer({'bootstrap.servers': KAFKA_BROKER})

def delivery_report(err, msg):
    """Delivery report for asynchronous produce."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic} [{msg.partition}] at offset {msg.offset}")

def simulate_video_frames():
    """Simulate streaming video frames to Kafka."""
    for frame_number in range(100):  # Simulate 100 frames
        frame = {
            'frame_number': frame_number,
            'data': f'frame_data_{frame_number}'  # Placeholder for actual frame data
        }
        producer.produce('video-frames', key=str(frame_number), value=json.dumps(frame), callback=delivery_report)
        producer.flush()  # Wait for any outstanding messages to be delivered
        time.sleep(0.1)  # Simulate delay between frames

if __name__ == "__main__":
    simulate_video_frames()
