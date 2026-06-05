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
        self.max_size_for_batch = 5000

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
            # the queue service can only give us an error is async - cancel occurs and need to marry up with task_done
            mes = await self.queue.get()
            try:
                self.batch_list.append(mes)

                if len(self.batch_list) >= self.max_size_for_batch:
                    # save the list location to a local variable
                    batch_to_write = self.batch_list
                    # create a new list and a new memory location
                    self.batch_list = []
                    normalised_list_of_data = self.exchange_adapter.normalise_data(batch_list=batch_to_write)

                    await asyncio.to_thread(
                        self.exchange_adapter.writer,
                        normalised_list_of_data
                    )
            except Exception as e:
                print(f"error normalising data and pushing to thread to save: \n{e}")
            finally:
                # a safety case if a errors occurs after the queue.get, thus this task_done balances the queue so we can move forward
                # technically not needed but if queue.join is used then this is required
                self.queue.task_done()
