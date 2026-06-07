import json
import websockets
import asyncio
import time

class WebsocketClient:
    def __init__(self, url, msg):
        self.ws = None
        self.url = url
        self.msg = msg

    async def shutdown(self):
        if self.ws:
            await self.ws.close()

    async def stream(self):
        delay = 1
        try:
            while True:
                try:
                    await self.connect()
                    await self.subscribe(self.msg)
                    delay = 1
                    async for message in self.ws:
                        data = json.loads(message)
                        sys_time = time.time()
                        data['sys_time'] = sys_time
                        yield data
                except Exception as e:
                    delay = min(delay * 2, 30)
                    print(f"Disconnected... {time.time()} \n{e}")
                    await asyncio.sleep(delay)
        except asyncio.CancelledError as e:
            print(f"Closing connection... in websocket, stream() {time.time()} \n{e}")
            await self.shutdown()
            raise




    async def connect(self):
        self.ws = await websockets.connect(self.url)
    # we can leave msg a parameter here if a additional message is required in the future
    async def subscribe(self, msg):
        await self.ws.send(json.dumps(msg))
