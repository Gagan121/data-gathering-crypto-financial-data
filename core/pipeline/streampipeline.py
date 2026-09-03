import asyncio
import time

from core.exchanges.exchange_adapter import ExchangeAdapter
from core.websockets.websocket_client import WebsocketClient
from working_example_concept import consumer


class StreamPipeline:
    def __init__(self, exchange_adapter:ExchangeAdapter):
        self.exchange_adapter = exchange_adapter

        self.ws = WebsocketClient(exchange_adapter=exchange_adapter)

        self.batch_list = []
        self.max_size_for_queue = 500
        self.max_size_for_batch = 500

        self.queue = dict()
        self.batch_list = dict()
        for channel in self.exchange_adapter.channels:
            self.queue[channel] = asyncio.Queue(maxsize=self.max_size_for_queue)
            self.batch_list[channel] = []

        self.consumer_tasks = dict()
        self.producer_task=None
        self._shutdown=False

    def get_exchange_adapter(self) -> ExchangeAdapter:
        return self.exchange_adapter

    def get_queue(self) -> dict:
        return self.queue

    async def subscribe_to_channels(self, channels:list, subscribe_message:dict):
        if not bool(subscribe_message):
            raise ValueError("subscribing message is none")
        for channel in channels:
            self.queue[channel] = asyncio.Queue(maxsize=self.max_size_for_batch)
            self.batch_list[channel] = []

            self.consumer_tasks[channel] = asyncio.create_task(self.consumer(channel))

        await self.ws.send_message_through_websocket_and_receive_message(msg=subscribe_message)


    async def unsubscribe_from_channels(self, channels:list, unsubscribe_message:dict):
        # content is present
        if not bool(unsubscribe_message):
            raise ValueError("unsubscribe message is none")

        await self.ws.send_message_through_websocket_and_receive_message(msg=unsubscribe_message)

        for channel in channels:
            task = self.consumer_tasks.pop(channel, None)

            if task is not None:
                task.cancel()

            await asyncio.gather(task, return_exceptions=True)

        for channel in channels:
            self.queue.pop(channel, None)
            self.batch_list.pop(channel, None)

        if not bool(self.queue):
            await self.shutdown()


    async def shutdown(self):

        if self._shutdown:
            return

        self._shutdown = True

        await self.ws.shutdown()
        if self.producer_task is not None:
            self.producer_task.cancel()
            # the gather here is waiting for the producer_task to stop and suppresses the error caused -> a try await could be similar catching a asyncio CancelledError
            await asyncio.gather(self.producer_task, return_exceptions=True)
        # making sure the producer ends first thus no messages can pass on to

        # thus making a copy stops the issue of making changes whilst iterating over them
        consumer_tasks = list(self.consumer_tasks.values())

        for task in consumer_tasks:
            task.cancel()
        await asyncio.gather(*consumer_tasks, return_exceptions=True)


    async def run(self):
        self.producer_task = asyncio.create_task(self.producer())
        # making a copy thus changes can be made to self.queue without errors
        for channel in list(self.queue.keys()):
            self.consumer_tasks[channel] = asyncio.create_task(self.consumer(channel))


        try:
            await asyncio.gather(
                self.producer_task,
                *self.consumer_tasks.values(),
            )
        except asyncio.CancelledError as e:
            print("asyncio.CancelledError, closing program: ",e)
            await self.shutdown()

            raise


    async def producer(self):
        try:
            async for msg in self.ws.stream():
                outcome = self.exchange_adapter.valid_message_can_pass_and_restructure_data(msg)
                if outcome['valid']:
                    # channel = "" -trades
                    if isinstance(outcome['data'],list):
                        channel = outcome['data'][-1]['channel']
                        for trade in outcome['data']:
                            await self.queue[channel].put(trade)
                    else:
                        # tickers
                        channel = outcome["data"]['channel']
                        await self.queue[channel].put(outcome['data'])

                        # await asyncio.sleep(0.1)
        except asyncio.CancelledError as e:
            print("asyncio.CancelledError producer, closing program: ",e)
            raise

    async def process_batch(self, channel):
        # save the list location to a local variable
        batch_to_write = self.batch_list[channel]
        try:
            # create a new list and a new memory location
            self.batch_list[channel] = []
            normalised_list_of_data = self.exchange_adapter.normalise_data(batch_list=batch_to_write)

            await asyncio.to_thread(
                self.exchange_adapter.writer,
                normalised_list_of_data
            )
        except Exception as e:
            print(f"error normalising data and pushing to thread to save: \n{e}")
            # if data fails to save we can carry it on to the original batch and try again on the next cycle
            self.batch_list[channel] = batch_to_write + self.batch_list[channel]

    async def consumer(self, channel):
        try:
            while True:
                # await here means wait until another value is here, async then says if nothing is left I'll put this to sleep
                mes = await self.queue[channel].get()
                try:
                    self.batch_list[channel].append(mes)

                    if len(self.batch_list[channel]) >= self.max_size_for_batch:
                        await self.process_batch(channel=channel)
                except Exception as e:
                    print("error on appending item to batch list: ", e)

                # a finally is required here as this task has to be done
                finally:
                    # technically not needed but if queue.join is used then this is required
                    self.queue[channel].task_done()

        except asyncio.CancelledError as e:
            print(f"asyncio.CancelledError in consumer {channel}, closing program: ", e)
            await self.shutdown()
            # required here to pass the error on forward through the program so all other async function can catch on
            raise