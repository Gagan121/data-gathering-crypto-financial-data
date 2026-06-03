from websockets.asyncio.client import connect
import asyncio

class StreamPipeline:
    def __init__(self, exchange_adapter, storage_adapter):
        self.exchange_adapter = exchange_adapter
        self.storage_adapter = storage_adapter

    async def producer(self):
        delay = 1
        while True:
            try:
                async with connect("wss://advanced-trade-ws.coinbase.com") as websocket:
                    subscribe_message = {
                        "type": "subscribe",
                        "product_ids": ["BTC-USD"],
                        "channel": "ticker"
                    }
                    await websocket.send(json.dumps(subscribe_message))

                    delay = 1

                    async for message in websocket:
                        data = json.loads(message)
                        if validate_message(data):
                            data['sys_time'] = time.time()
                            await queue.put(data)
            except Exception as e:
                print(f"Disconnected... {time.time()} \n{e}")
                await asyncio.sleep(delay)
                # increasing delay, by a factor of 2 and till get gets to the limit of 30 seconds and stays there
                delay = min(delay * 2, 30)
                # print("test")
        #             need to pass this into a async queue and then saved to file


