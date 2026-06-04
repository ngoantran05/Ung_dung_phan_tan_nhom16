from kafka import KafkaConsumer
import json
from collections import defaultdict

stats = defaultdict(int)

consumer = KafkaConsumer(
    'sensor-data',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m:
        json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest'
)

print("MASTER STARTED")

for message in consumer:

    data = message.value

    sensor = data["sensor_id"]

    stats[sensor] += 1

    print("\n===== MASTER REPORT =====")

    for key, value in stats.items():
        print(
            f"{key}: {value} messages"
        )