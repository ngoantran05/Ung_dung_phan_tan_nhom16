from kafka import KafkaConsumer
import json
from collections import defaultdict
count = defaultdict(int)
total = defaultdict(int)
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
    value = data["value"]
    count[sensor] += 1
    total[sensor] += value
    print("\n===== MASTER REPORT =====")
    for key in count:
        avg = total[key] / count[key]
        print(f"{key}: {count[key]} messages")
        print(f"Average Value: {avg:.2f}")

