import cv2
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9093')
cap = cv2.VideoCapture(0)

while True:
    ret, frame = cap.read()
    if not ret:
        break
    _, buffer = cv2.imencode('.jpg', frame)
    producer.send('webcam-frames', buffer.tobytes())

cap.release()
