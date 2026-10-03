import ast
from typing import override
import asyncio
from core.complex_exchanges.polymarket_adapter import PolymarketConfig, PolymarketAdapter
from core.manager.manage_subscription import ManageSubscription
import copy
import math
from core.pipeline.streampipeline import StreamPipeline


class PolymarketManager(ManageSubscription[PolymarketConfig]):
    def __init__(self, pipelines: list, limit_of_number_of_channels:int):
        super().__init__(pipelines=pipelines, limit_of_number_of_channels=limit_of_number_of_channels)

        self.exchange_type = PolymarketAdapter

    @classmethod
    def generate_multiple_adapters(cls, polymarket_config:PolymarketConfig) -> list:

        information = PolymarketAdapter.get_instruments(config=polymarket_config)
        # true if information is there
        if not (bool(information)):
            return []

        list_of_instruments_dict = PolymarketAdapter.sort_data_form_new_requests(information)
        # list of instruments dict is mapped with the ticker to the event
        total_channels_dict = cls.format_instruments_to_channels(list_of_instruments_dict=list_of_instruments_dict, config=polymarket_config)

        total_channels_list = list(total_channels_dict.keys())

        list_of_lists_of_channels = [total_channels_list[x:x + polymarket_config.limit_number_of_channels] for x in range(0, len(total_channels_list), polymarket_config.limit_number_of_channels)]
        list_of_adapters = []
        for i in range(len(list_of_lists_of_channels)):
            # we have to create a copy here otherwise pass by reference would make all the msg the same
            adapter_msg = copy.deepcopy(polymarket_config.msg)
            adapter_msg["assets_ids"] = [item for item in list_of_lists_of_channels[i]]

            list_of_adapters.append(
                PolymarketAdapter(
                    # given we are using pass by reference we are not creating multiple copies so this i okay -> we have multiple channels per adapter so having all the event reference is a okay/safe design choice
                    channels=adapter_msg["assets_ids"],
                    exchange_info=None,
                    exchange_name=polymarket_config.exchange_name,
                    websocket_url=polymarket_config.websocket_url,
                    msg=adapter_msg,
                    base_url=polymarket_config.base_url,
                    heart_beat_msg= None,
                    heart_beat_reply_msg= None,
                )
            )

        PolymarketAdapter.add_dict_to_event_info_dict(event_dict=total_channels_dict)

        return list_of_adapters


    @staticmethod
    def convert_data_to_single_list_of_channels(data:list) -> list:
        list_of_instruments = []
        for instrument_dict in data:
            if "instrument_name" not in instrument_dict:
                continue
            instrument_name = instrument_dict["instrument_name"]
            list_of_instruments.append(instrument_name)
        return list_of_instruments

    @staticmethod
    def format_instruments_to_channels(list_of_instruments_dict: list, config: PolymarketConfig):
        total_channels_dict = {}
        for item in list_of_instruments_dict:
            try:
                clobTokenIds = ast.literal_eval(item['markets'][0]["clobTokenIds"])
                outcomes = ast.literal_eval(item['markets'][0]["outcomes"])

                item['markets'][0]["clobTokenIds"] = clobTokenIds
                item['markets'][0]["outcomes"] = outcomes

            except Exception as e:
                print(e, " issue converting the outcome to a python list, for polymarket instrument to channel formatting")
                raise

            for i in range(len(clobTokenIds)):
                total_channels_dict[clobTokenIds[i]] = item

        return total_channels_dict


    def find_instruments(self, config:PolymarketConfig) -> list:
        exchange_with_expiry_type = config.get_exchange_adapter_type()

        if not issubclass(exchange_with_expiry_type, PolymarketAdapter):
            return []

        information = exchange_with_expiry_type.get_instruments(config=config)
        list_of_instruments_dict = exchange_with_expiry_type.sort_data_form_new_requests(information)

        return list_of_instruments_dict

    @override
    def compare_pipelines_with_newly_gathered_instruments(self, list_of_instruments) -> dict:
        set_of_requested_instruments = set(list_of_instruments.keys())
        set_of_instruments_have_already = set(self.dict_of_channel_to_pipeline.keys())

        required = set_of_requested_instruments - set_of_instruments_have_already
        remove = set_of_instruments_have_already - set_of_requested_instruments

        # # -------------------------------------------------------- these two lines need to be removed
        # required = set(list(set_of_requested_instruments)[:2])
        # remove = set(list(set_of_instruments_have_already)[:2])
        # # -------------------------------------------------------- these two lines above need to be removed

        try:
            required_subset = { item :list_of_instruments[item] for item in required}
            remove_subset = { item :PolymarketAdapter.event_info.get(item) for item in remove}
        except Exception as e:
            print("issue with getting key, to event info", e)
            raise ValueError("Issue with getting key, to event info")

        return {
            "required": required_subset,
            "remove": remove_subset,
        }

    @override
    async def add_channels(self, channels_to_acquire:dict , config: PolymarketConfig):

        channels_to_acquire_list = list(channels_to_acquire.keys())

        for pipeline in self.pipelines:

            if len(channels_to_acquire_list) <= 0:
                return

            exchange_with_expiry = pipeline.get_exchange_adapter()
            if isinstance(exchange_with_expiry, PolymarketAdapter):
                list_of_channels_on_exchange = exchange_with_expiry.channels

                number_of_empty_channel_spaces = self.limit_of_number_of_channels - len(list_of_channels_on_exchange)

                if number_of_empty_channel_spaces <= 0:
                    continue

                # space =  len(dict_of_channels_to_acquire_and_remove["required"]) - number_of_empty_channel_spaces

                section_of_channels = channels_to_acquire_list[:number_of_empty_channel_spaces]

                await self.place_channels_into_pipeline(pipeline=pipeline,channels=section_of_channels, dict_channel_to_full_channel=channels_to_acquire)

                channels_to_acquire_list = channels_to_acquire_list[number_of_empty_channel_spaces:]
        # designed so we can create new adapters -> remove lat


        if len(channels_to_acquire_list) > 0:

            number_of_loops = math.ceil(len(channels_to_acquire_list) / self.limit_of_number_of_channels)

            for i in range(number_of_loops):
                grouped_channels_to_acquire = channels_to_acquire_list[:self.limit_of_number_of_channels]
                self.create_new_pipeline_to_handle_new_channels(channels=grouped_channels_to_acquire, config=config, dict_channel_to_full_channel=channels_to_acquire)
                channels_to_acquire_list = channels_to_acquire_list[self.limit_of_number_of_channels:]

    #               you have to have this value like this                               dict_channel_to_full_channel is none and therefore links back to the parent
    @override
    async def place_channels_into_pipeline(self, pipeline:StreamPipeline, channels:list, dict_channel_to_full_channel:dict|None = None):
        # channels is empty
        if len(channels) <= 0:
            return

        if dict_channel_to_full_channel is None:
            raise ValueError("dict_channel_to_full_channel cannot be None, in polymarket manager")

        exchange_with_expiry = pipeline.get_exchange_adapter()
        if not isinstance(exchange_with_expiry, PolymarketAdapter):
            return
        subscribe_message = exchange_with_expiry.get_subscribe_to_additional_channel_msg(channels=channels)
        await pipeline.subscribe_to_channels(channels=channels,subscribe_message=subscribe_message)
        # add added channels to the total channels under pipeline
        total_channels_in_pipeline = exchange_with_expiry.get_channels() + channels
        exchange_with_expiry.set_channels(total_channels_in_pipeline)

        # filtering the key, values we are going to keep -> so the dictionary is not the full amount as channels is reduced
        subset_dict_channel_to_full_channel = { k:dict_channel_to_full_channel[k] for k in channels}

        exchange_with_expiry.add_dict_to_event_info_dict(event_dict=subset_dict_channel_to_full_channel)


    @override
    def create_new_pipeline_to_handle_new_channels(self, channels:list, config: PolymarketConfig, dict_channel_to_full_channel:dict|None = None):
        if len(channels) <= 0: return

        if dict_channel_to_full_channel is None:
            raise ValueError("dict_channel_to_full_channel cannot be None, in polymarket manager")

        exchange_with_expiry_type = config.get_exchange_adapter_type()
        if not issubclass(exchange_with_expiry_type, PolymarketAdapter):
            return

        subset_dict_channel_to_full_channel = {k: dict_channel_to_full_channel[k] for k in channels}
        new_exchange_with_expiry = exchange_with_expiry_type.create_new_adapter(channels=subset_dict_channel_to_full_channel, config=config)

        new_exchange_with_expiry.set_channels_in_msg()

        new_pipeline = StreamPipeline(new_exchange_with_expiry)
        self.pipelines.append(new_pipeline)

        task = asyncio.create_task(new_pipeline.run())
        self.pipeline_tasks.append(task)


    @override
    async def remove_channels(self, channels_to_remove):

        channels_to_remove_list = list(channels_to_remove.keys())

        dict_pipeline_to_channels_list_to_remove = dict()
        for channel in channels_to_remove_list:

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

            if not isinstance(exchange_with_expiry, PolymarketAdapter):
                continue
            unsubscribe_message = exchange_with_expiry.get_unsubscribe_from_channel_msg(channels=channels)
            await pipeline.unsubscribe_from_channels(channels=channels, unsubscribe_message=unsubscribe_message)

            # remove the channels from the adapters
            list_of_channels_to_store = list(set(exchange_with_expiry.get_channels()) - set(channels))
            exchange_with_expiry.set_channels(list_of_channels_to_store)

            for single_channel in channels:
                event_info = channels_to_remove[single_channel]
                outcome = exchange_with_expiry.remove_event_info_to_dict(key=single_channel)
                if outcome != event_info:
                    print("outcome did not equal event_info, therefore the event_info was not removed or was never in the event_info in polymarketadapter")


            if not bool(pipeline.get_queue()):
                self.pipelines.remove(pipeline)

