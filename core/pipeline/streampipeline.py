import asyncio

from core.exchanges.exchange_adapter import ExchangeAdapter
from core.websockets.websocket_client import WebsocketClient


class StreamPipeline:
    def __init__(self, exchange_adapter:ExchangeAdapter):
        self.exchange_adapter = exchange_adapter

        self.ws = WebsocketClient(exchange_adapter=exchange_adapter)

        self.batch_list = []
        self.max_size_for_queue = 1000
        self.max_size_for_batch = 5000

        self.queue = asyncio.Queue(maxsize=self.max_size_for_queue)



    async def run(self):
        producer_task = asyncio.create_task(self.producer())
        consumer_task = asyncio.create_task(self.consumer())
        try:

            await asyncio.gather(
                producer_task,
                consumer_task,
            )
        except asyncio.CancelledError as e:
            print("asyncio.CancelledError, closing program: ",e)
            await self.ws.shutdown()

            producer_task.cancel()
            # the gather here is waiting for the producer_task to stop and suppresses the error caused -> a try await could be similar catching a asyncio CancelledError
            await asyncio.gather(producer_task, return_exceptions=True)
            # making sure the producer ends first thus no messages can pass on to

            consumer_task.cancel()
            await asyncio.gather(consumer_task, return_exceptions=True)


    async def producer(self):
        try:
            async for msg in self.ws.stream():
                outcome = self.exchange_adapter.valid_message_can_pass_and_restructure_data(msg)
                if outcome['valid']:
                    await self.queue.put(outcome['data'])
        except asyncio.CancelledError as e:
            print("asyncio.CancelledError producer, closing program: ",e)
            raise

    async def process_batch(self):
        # save the list location to a local variable
        batch_to_write = self.batch_list
        try:
            # create a new list and a new memory location
            self.batch_list = []
            normalised_list_of_data = self.exchange_adapter.normalise_data(batch_list=batch_to_write)

            await asyncio.to_thread(
                self.exchange_adapter.writer,
                normalised_list_of_data
            )
        except Exception as e:
            print(f"error normalising data and pushing to thread to save: \n{e}")
            # if data fails to save we can carry it on to the original batch and try again on the next cycle
            self.batch_list = batch_to_write + self.batch_list

    async def consumer(self):
        try:
            while True:
                mes = await self.queue.get()
                try:
                    self.batch_list.append(mes)

                    if len(self.batch_list) >= self.max_size_for_batch:
                        await self.process_batch()
                except Exception as e:
                    print("error on appending item to batch list: ", e)

                # a finally is required here as this task has to be done
                finally:
                    # technically not needed but if queue.join is used then this is required
                    self.queue.task_done()

        except asyncio.CancelledError as e:
            print("asyncio.CancelledError in consumer, closing program: ",e)


            # we need to empty the queue of all remaining items
            while True:
                try:
                    mes = self.queue.get_nowait()
                except asyncio.QueueEmpty as e:
                    break
                else:
                    self.queue.task_done()
                    self.batch_list.append(mes)

            if self.batch_list:
                await self.process_batch()
            # required here to pass the error on forward through the program so all other async function can catch on
            raise