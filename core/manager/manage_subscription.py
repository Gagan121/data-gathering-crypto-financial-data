import asyncio
import time

from pandas.conftest import rand_series_with_duplicate_datetimeindex
import copy
from core.complex_exchanges.exchanges_with_expiry import ExchangeWithExpiry
from abc import abstractmethod

from core.pipeline.streampipeline import StreamPipeline


class ManageSubscription:
    def __init__(self, pipelines: list, limit_of_number_of_channels:int):
        self.pipelines = pipelines
        self.pipeline_tasks = []
        self.limit_of_number_of_channels = limit_of_number_of_channels
        self.dict_of_channel_to_pipeline = self.decompile_channels_to_pipeline()


    @abstractmethod
    def find_instruments(self):
        pass

    def decompile_channels_to_pipeline(self):
        channel_to_adapter = dict()
        for i in range(len(self.pipelines)):
            for channel in self.pipelines[i].get_exchange_adapter().get_channels():
                channel_to_adapter[channel] = i

        return channel_to_adapter


    def compare_pipelines_with_newly_gathered_instruments(self, list_of_instruments):

        set_of_requested_instruments = set(list_of_instruments)
        set_of_instruments_have_already = set(self.dict_of_channel_to_pipeline.keys())

        required = set_of_requested_instruments - set_of_instruments_have_already
        remove = set_of_instruments_have_already - set_of_requested_instruments

        return {
            "required": required,
            "remove": remove,
        }

    async def remove_channels(self, channels_to_remove):
        dict_pipeline_to_channels_list_to_remove = dict()
        for channel in channels_to_remove:
            pipeline_index = self.dict_of_channel_to_pipeline[channel]

            if pipeline_index not in dict_pipeline_to_channels_list_to_remove:
                dict_pipeline_to_channels_list_to_remove[pipeline_index] = [channel]
            else:
                dict_pipeline_to_channels_list_to_remove[pipeline_index].append(channel)


        for pipeline_index in dict_pipeline_to_channels_list_to_remove.keys():
            channels = dict_pipeline_to_channels_list_to_remove[pipeline_index]

            pipeline = self.pipelines[pipeline_index]

            exchange_with_expiry = pipeline.get_exchange_adapter()

            if not isinstance(exchange_with_expiry, ExchangeWithExpiry):
                continue
            unsubscribe_message = exchange_with_expiry.get_unsubscribe_from_channel_msg(channels=channels)
            await pipeline.unsubscribe_from_channels(channels=channels, unsubscribe_message=unsubscribe_message)

            # remove the channels from the adapters
            list_of_channels_to_store = list(set(exchange_with_expiry.get_channels()) - set(channels))
            exchange_with_expiry.set_channels(list_of_channels_to_store)

            if not bool(pipeline.get_queue()):
                self.pipelines.remove(pipeline)


    #         check if pipeline has any channels in it if not then remove the whole pipeline -> done through a internal check in the pipeline

    def place_channels_into_pipeline(self, pipeline:StreamPipeline, channels:list):
        exchange_with_expiry = pipeline.get_exchange_adapter()
        if not isinstance(exchange_with_expiry, ExchangeWithExpiry):
            return
        subscribe_message = exchange_with_expiry.get_subscribe_to_channel_msg(channels=channels)
        pipeline.subscribe_to_channels(channels=channels,subscribe_message=subscribe_message)


    def create_new_pipeline_to_handle_new_channels(self, pipeline:StreamPipeline, channels:list):
        exchange_with_expiry = pipeline.get_exchange_adapter()
        if not isinstance(exchange_with_expiry, ExchangeWithExpiry):
            raise ValueError("create new pipeline parent is no ExchangeWithExpiry")

        new_exchange_with_expiry = exchange_with_expiry.create_new_adapter(channels=channels)
        new_exchange_with_expiry.set_channels_in_msg()

        new_pipeline = StreamPipeline(new_exchange_with_expiry)
        self.pipelines.append(new_pipeline)

        task = asyncio.create_task(new_pipeline.run())
        self.pipeline_tasks.append(task)



    async def add_channels(self, channels_to_acquire):
        for pipeline in self.pipelines:
            exchange_with_expiry = pipeline.get_exchange_adapter()
            if isinstance(exchange_with_expiry, ExchangeWithExpiry):
                list_of_channels_on_exchange = exchange_with_expiry.channels

                number_of_empty_channel_spaces = self.limit_of_number_of_channels - len(list_of_channels_on_exchange)

                if number_of_empty_channel_spaces <= 0:
                    continue

                # space =  len(dict_of_channels_to_acquire_and_remove["required"]) - number_of_empty_channel_spaces

                section_of_channels = channels_to_acquire[:number_of_empty_channel_spaces]

                self.place_channels_into_pipeline(pipeline,section_of_channels)

                channels_to_acquire = channels_to_acquire[number_of_empty_channel_spaces:]

        if len(channels_to_acquire) > 0:
            if len(self.pipelines) <= 0:
                raise ValueError("could not create a new pipeline as none exist")
            pipeline = self.pipelines[0]
            self.create_new_pipeline_to_handle_new_channels(pipeline=pipeline, channels=channels_to_acquire)
    #


    async def shutdown(self):
        for task in self.pipeline_tasks:
            task.cancel()

        await asyncio.gather(*self.pipeline_tasks, return_exceptions=True)

        self.pipeline_tasks.clear()




    async def run(self):
        try:
            while True:
                time.sleep(60)
                list_of_instruments = self.find_instruments()
                dict_of_channels_to_acquire_and_remove = self.compare_pipelines_with_newly_gathered_instruments(list_of_instruments)

                await self.remove_channels(channels_to_remove=dict_of_channels_to_acquire_and_remove['remove'])

                await self.add_channels(channels_to_acquire=dict_of_channels_to_acquire_and_remove["required"])

        except asyncio.CancelledError as e:
            print(f"asyncio.CancelledError in run in manager_subscription, closing program: ", e)
            await self.shutdown()
            # required here to pass the error on forward through the program so all other async function can catch on
            raise








