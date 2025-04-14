# producer.py
from kafka import KafkaProducer
import time
import json
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    data = {
        "sensor_id": random.randint(1, 5),
        "value": round(random.uniform(20.0, 40.0), 2),
        "timestamp": time.time()
    }
    producer.send('test-topic', value=data)
    print(f"Enviado: {data}")
    time.sleep(1)
