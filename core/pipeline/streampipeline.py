import asyncio

from core.exchanges.exchange_adapter import ExchangeAdapter
from core.websockets.websocket_client import WebsocketClient


class StreamPipeline:
    def __init__(self, exchange_adapter:ExchangeAdapter):
        self.exchange_adapter = exchange_adapter
        self.ws = WebsocketClient(
            url=self.exchange_adapter.get_url(),
            msg=self.exchange_adapter.get_msg()
        )

        self.batch_list = []
        self.max_size_for_queue = 1000

        self.queue = asyncio.Queue(maxsize=self.max_size_for_queue)
        self.normalised_list_of_data = []



    async def run(self):
        await asyncio.gather(
            self.producer(),
            self.consumer(),
        )

    async def producer(self):
        async for msg in self.ws.stream():
            if self.exchange_adapter.validate_message(msg):
                await self.queue.put(msg)

    async def consumer(self):
        while True:
            mes = await self.queue.get()
            self.batch_list.append(mes)

            if len(self.batch_list) >= (self.max_size_for_queue -1):
                normalised_list_of_data = self.exchange_adapter.normalise_data(batch_list=self.batch_list)

                await asyncio.to_thread(
                    self.exchange_adapter.writer,
                    normalised_list_of_data
                )
                self.batch_list.clear()

