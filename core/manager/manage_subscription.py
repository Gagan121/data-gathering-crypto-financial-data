import asyncio
import math
import time

import copy
from typing import TypeVar, Generic

from core.complex_exchanges.deribit_options_adapter import DeribitOptionsConfig
from core.complex_exchanges.exchanges_with_expiry import ExchangeWithExpiry, ExchangeConfig
from abc import ABC, abstractmethod

from core.pipeline.streampipeline import StreamPipeline

T = TypeVar('T')

class ManageSubscription(ABC, Generic[T]):
    def __init__(self, pipelines: list, limit_of_number_of_channels:int):
        self.pipelines = pipelines
        self.pipeline_tasks = []
        self.limit_of_number_of_channels = limit_of_number_of_channels
        self.dict_of_channel_to_pipeline = self.decompile_channels_to_pipeline()

    @classmethod
    @abstractmethod
    def generate_multiple_adapters(cls, exchange_with_expiry:ExchangeWithExpiry) -> list:
        pass

    @abstractmethod
    def find_instruments(self, config: ExchangeConfig) -> list:
        pass

    @staticmethod
    @abstractmethod
    def format_instruments_to_channels(list_of_instruments_dict: list, config:T):
        pass

    @staticmethod
    @abstractmethod
    def convert_data_to_single_list_of_channels(data:list) -> list:
        pass

    def set_dict_of_channels_to_pipeline(self, dict_of_channels_to_pipeline: dict):
        self.dict_of_channel_to_pipeline = dict_of_channels_to_pipeline


    def decompile_channels_to_pipeline(self):
        channel_to_adapter = dict()
        for i in range(len(self.pipelines)):
            pipeline = self.pipelines[i]
            for channel in pipeline.get_exchange_adapter().get_channels():
                channel_to_adapter[channel] = pipeline

        return channel_to_adapter




    def compare_pipelines_with_newly_gathered_instruments(self, list_of_instruments) -> dict[str, list[str]]:

        set_of_requested_instruments = set(list_of_instruments)
        set_of_instruments_have_already = set(self.dict_of_channel_to_pipeline.keys())

        required = set_of_requested_instruments - set_of_instruments_have_already
        remove = set_of_instruments_have_already - set_of_requested_instruments

        return {
            "required": list(required),
            "remove": list(remove),
        }

    async def remove_channels(self, channels_to_remove):
        dict_pipeline_to_channels_list_to_remove = dict()
        for channel in channels_to_remove:

            if channel not in self.dict_of_channel_to_pipeline.keys():
                continue

            pipeline = self.dict_of_channel_to_pipeline[channel]


            if pipeline not in dict_pipeline_to_channels_list_to_remove:
                dict_pipeline_to_channels_list_to_remove[pipeline] = [channel]
            else:
                dict_pipeline_to_channels_list_to_remove[pipeline].append(channel)


        for pipeline in dict_pipeline_to_channels_list_to_remove.keys():
            channels = dict_pipeline_to_channels_list_to_remove[pipeline]

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

    async def place_channels_into_pipeline(self, pipeline:StreamPipeline, channels:list):
        # channels is empty
        if len(channels) <= 0:
            return
        exchange_with_expiry = pipeline.get_exchange_adapter()
        if not isinstance(exchange_with_expiry, ExchangeWithExpiry):
            return
        subscribe_message = exchange_with_expiry.get_subscribe_to_channel_msg(channels=channels)
        await pipeline.subscribe_to_channels(channels=channels,subscribe_message=subscribe_message)
        # add added channels to the total channels under pipeline
        total_channels_in_pipeline = exchange_with_expiry.get_channels() + channels
        exchange_with_expiry.set_channels(total_channels_in_pipeline)


    def create_new_pipeline_to_handle_new_channels(self, channels:list, config: ExchangeConfig):
        if len(channels) <= 0: return
        exchange_with_expiry_type = config.get_exchange_adapter_type()
        if not issubclass(exchange_with_expiry_type, ExchangeWithExpiry):
            return
        new_exchange_with_expiry = exchange_with_expiry_type.create_new_adapter(channels=channels, config=config)
        new_exchange_with_expiry.set_channels_in_msg()

        new_pipeline = StreamPipeline(new_exchange_with_expiry)
        self.pipelines.append(new_pipeline)

        task = asyncio.create_task(new_pipeline.run())
        self.pipeline_tasks.append(task)



    async def add_channels(self, channels_to_acquire:list , config: ExchangeConfig):

        for pipeline in self.pipelines:

            if len(channels_to_acquire) <= 0:
                return

            exchange_with_expiry = pipeline.get_exchange_adapter()
            if isinstance(exchange_with_expiry, ExchangeWithExpiry):
                list_of_channels_on_exchange = exchange_with_expiry.channels

                number_of_empty_channel_spaces = self.limit_of_number_of_channels - len(list_of_channels_on_exchange)

                if number_of_empty_channel_spaces <= 0:
                    continue

                # space =  len(dict_of_channels_to_acquire_and_remove["required"]) - number_of_empty_channel_spaces

                section_of_channels = channels_to_acquire[:number_of_empty_channel_spaces]

                await self.place_channels_into_pipeline(pipeline,section_of_channels)

                channels_to_acquire = channels_to_acquire[number_of_empty_channel_spaces:]

        # channels_to_acquire = ['ticker.BTC-6SEP26-68000-P.agg2']

        if len(channels_to_acquire) > 0:

            number_of_loops = math.ceil(len(channels_to_acquire) / self.limit_of_number_of_channels)

            for i in range(number_of_loops):
                grouped_channels_to_acquire = channels_to_acquire[:self.limit_of_number_of_channels]
                self.create_new_pipeline_to_handle_new_channels(channels=grouped_channels_to_acquire, config=config)
                channels_to_acquire = channels_to_acquire[self.limit_of_number_of_channels:]


    async def shutdown(self):
        for task in self.pipeline_tasks:
            task.cancel()

        await asyncio.gather(*self.pipeline_tasks, return_exceptions=True)

        self.pipeline_tasks.clear()



    async def run(self, config: ExchangeConfig):
        try:
            while True:
                await asyncio.sleep(60)

                list_of_instruments_dict = self.find_instruments(config=config)

                list_of_channels = self.format_instruments_to_channels(list_of_instruments_dict, config)
                # get an accurate image of what channels are where
                self.dict_of_channel_to_pipeline = self.decompile_channels_to_pipeline()
                dict_of_channels_to_acquire_and_remove = self.compare_pipelines_with_newly_gathered_instruments(list_of_channels)

                print(dict_of_channels_to_acquire_and_remove)

                # dict_of_channels_to_acquire_and_remove["remove"] = ['ticker.BTC-6SEP26-68000-C.agg2','ticker.BTC-6SEP26-68000-P.agg2']

                await self.remove_channels(channels_to_remove=dict_of_channels_to_acquire_and_remove['remove'])

                # dict_of_channels_to_acquire_and_remove["required"] = ['ticker.BTC-6SEP26-68000-C.agg2']

                await self.add_channels(channels_to_acquire=dict_of_channels_to_acquire_and_remove["required"], config=config)

                # await asyncio.sleep(10)
        except Exception as e:
            print(e)

        except asyncio.CancelledError as e:
            print(f"asyncio.CancelledError in run in manager_subscription, closing program: ", e)
            await self.shutdown()
            # required here to pass the error on forward through the program so all other async function can catch on
            raise








