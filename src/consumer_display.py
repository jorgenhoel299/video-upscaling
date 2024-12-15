from kafka import KafkaConsumer
import cv2
import numpy as np

consumer = KafkaConsumer(
    'processed-frames',
    bootstrap_servers='localhost:9093',
    auto_offset_reset='earliest'
)

for message in consumer:
    frame_bytes = message.value
    nparr = np.frombuffer(frame_bytes, np.uint8)
    frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    cv2.imshow('Upscaled Frame', frame)
    if cv2.waitKey(1) & 0xFF == ord('q'):
        break

cv2.destroyAllWindows()
