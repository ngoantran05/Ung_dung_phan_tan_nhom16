from kafka import KafkaProducer
import json
import random
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v:
        json.dumps(v).encode('utf-8')
)

while True:

    data = {
        "sensor_id": f"sensor_{random.randint(1,3)}",
        "value": random.randint(10,100)
    }

    producer.send(
        "sensor-data",
        data
    )

    print("[SEND]", data)

    time.sleep(2)