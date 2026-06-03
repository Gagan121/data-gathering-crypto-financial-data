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

    async def run(self):
        await asyncio.gather(
            self.producer(),
            self.consumer(),
        )

    async def producer(self):
        async for msg in self.ws.stream():
            if self.exchange_adapter.validate_message(msg):
                await self.exchange_adapter.add_to_queue(msg)

    async def consumer(self):
        while True:
            mes = await self.exchange_adapter.get_item_from_queue()
            self.exchange_adapter.add_to_batch_list(mes)

            if self.exchange_adapter.check_batch_size_reach_max():
                self.exchange_adapter.normalise_data()
                self.exchange_adapter.clear_batch_list()

                await asyncio.to_thread(
                    self.exchange_adapter.writer
                )

