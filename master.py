from kafka import KafkaConsumer
import json
from collections import defaultdict

consumer = KafkaConsumer(
    'sensor-data',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest'
)

count = defaultdict(int)
total = defaultdict(int)

for message in consumer:

    data = message.value

    sensor = data["sensor_id"]
    value = data["value"]

    count[sensor] += 1
    total[sensor] += value

    print("\n===== MASTER REPORT =====")

    for s in count:

        avg = total[s] / count[s]

        print(
            f"{s}: {count[s]} messages | Average = {avg:.2f}"
        )