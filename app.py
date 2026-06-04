import faust

app = faust.App(
    'distributed_stream_app',
    broker='kafka://localhost:9092'
)

class Sensor(faust.Record, serializer='json'):
    sensor_id: str
    value: float

topic = app.topic('sensor-data', value_type=Sensor)

high_value_table = app.Table(
    'high_value_count',
    default=int
)

@app.agent(topic)
async def process(stream):

    async for event in stream:

        print(
            f"[PROCESS] {event.sensor_id} : {event.value}"
        )

        if event.value > 50:
            high_value_table[event.sensor_id] += 1

            print(
                f"[WARNING] {event.sensor_id} "
                f"high value = {event.value}"
            )

@app.timer(interval=10.0)
async def statistics():

    print("\n===== REAL TIME STATS =====")

    for key, value in high_value_table.items():
        print(
            f"{key} -> {value} high values"
        )

if __name__ == '__main__':
    app.main()