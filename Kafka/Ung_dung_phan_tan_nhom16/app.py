import faust

app = faust.App(
    'demo',
    broker='kafka://localhost:9092'
)

topic = app.topic('test')

@app.agent(topic)
async def process(stream):
    async for value in stream:
        print("Received:", value)

if __name__ == '__main__':
    app.main()