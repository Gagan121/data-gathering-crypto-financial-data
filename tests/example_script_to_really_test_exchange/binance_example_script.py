import asyncio
import json
import websockets

async def main():
    ws = await websockets.connect("wss://fstream.binance.com/ws")

    await ws.send(json.dumps({
        "method": "SUBSCRIBE",
        "params": ["btcusdt@markPrice@1s"],
        "id": 1
    }))

    print(await ws.recv())  # subscription ack

    while True:
        msg = await ws.recv()
        print(msg)

asyncio.run(main())