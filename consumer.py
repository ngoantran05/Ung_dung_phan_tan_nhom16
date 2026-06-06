from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'sensor-data',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest'
)

for message in consumer:

    data = message.value

    print("[CONSUMER]", data)

    if data["value"] > 80:
        print(
            f"[WARNING] {data['sensor_id']} abnormal value: {data['value']}"
        )