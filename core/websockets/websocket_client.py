import json
import websockets
import asyncio
import time

class WebsocketClient:
    def __init__(self, url, msg):
        self.ws = None
        self.url = url
        self.msg = msg

    async def stream(self):
        delay = 1
        while True:
            try:
                await self.connect()
                await self.subscribe(self.msg)
                delay = 1
                async for message in self.ws:
                    sys_time = time.time()
                    if isinstance(message, dict):
                        message['sys_time'] = sys_time
                    yield message
            except Exception as e:
                delay = min(delay * 2, 30)
                print(f"Disconnected... {time.time()} \n{e}")
                await asyncio.sleep(delay)



    async def connect(self):
        self.ws = websockets.connect(self.url)
    # we can leave msg a parameter here if a additional message is required in the future
    async def subscribe(self, msg):
        self.ws.send(json.dumps(msg))
