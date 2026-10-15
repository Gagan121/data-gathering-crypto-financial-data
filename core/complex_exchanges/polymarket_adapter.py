import json
import time
import gzip
from datetime import datetime
from decimal import Decimal
from typing import override
import pandas as pd
from core.exchanges.exchange_adapter import flatten, convert_to_decimal_and_quantize
from core.complex_exchanges.exchanges_with_expiry import ExchangeWithExpiry, ExchangeConfig
from core.rest_requests.rest_client_requests import RestClient
import os
from dotenv import load_dotenv
from urllib.parse import urljoin
import copy
from dataclasses import dataclass, field, fields
import re
import ast

load_dotenv()

@dataclass
class PolymarketConfig(ExchangeConfig):
    limit_number_of_channels: int
    interval_type:str
    base_url: str
    msg: dict
    websocket_url: str
    exchange_name: str
    series_id: str

    def get_exchange_adapter_type(self) -> type:
        return PolymarketAdapter


    def __post_init__(self):

        for single_field in fields(self):
            if getattr(self, single_field.name) is None:
                raise ValueError(f"{single_field.name} cannot be None")

        if self.limit_number_of_channels <= 0:
            raise ValueError("limit_number_of_channels must be greater than 0")

        if self.limit_number_of_channels >= 500:
            raise ValueError("limit_number_of_channels must be smaller 500")

        if self.interval_type not in ("agg2", "raw", "100ms"):
            raise ValueError("invalid interval_type")



#         check all values are not None


class PolymarketAdapter(ExchangeWithExpiry):

    event_info = {}

    # used to get the number of instruments we are looking for
    def __init__(self, base_url:str, exchange_info:dict, channels: list, exchange_name: str, websocket_url: str, msg: dict,
                 heart_beat_msg: dict, heart_beat_reply_msg:dict) -> None:

        super().__init__(base_url=base_url, exchange_info=exchange_info, channels=channels, exchange_name=exchange_name, websocket_url=websocket_url, msg=msg, ticker=None, heart_beat_msg=heart_beat_msg, heart_beat_reply_msg=heart_beat_reply_msg)
        load_dotenv()

        self._client_id = os.getenv("")
        self._client_secret = os.getenv("")

    @classmethod
    def add_dict_to_event_info_dict(cls, event_dict):
        cls.event_info.update(event_dict)

    @classmethod
    def remove_event_info_to_dict(cls, key):
        return cls.event_info.pop(key, None)




    @staticmethod
    def create_new_adapter(channels:dict, config:PolymarketConfig) -> ExchangeWithExpiry:

        clobTokenIds = list(channels.keys())
        PolymarketAdapter.add_dict_to_event_info_dict(channels)

        return (config.get_exchange_adapter_type())(
            base_url=config.base_url,
            exchange_info=None,
            channels=clobTokenIds,
            exchange_name=config.exchange_name,
            websocket_url=config.websocket_url,
            msg=copy.deepcopy(config.msg),
            heart_beat_msg=None,
            heart_beat_reply_msg=None,
        )



    def add_request_id(self, msg:dict, request_id) -> dict:
        if (msg is not None) and isinstance(msg, dict):
            msg["id"] = request_id

        return msg


    @staticmethod
    def sort_data_form_new_requests(information) -> list:

        data_is_valid = (
                isinstance(information, list)
                and len(information) > 0
                and "id" in information[0]
                and "markets" in information[0]
                and "ticker" in information[0]
                and isinstance(information[0]["markets"], list)
                and "outcomes" in information[0]["markets"][0]
                and "clobTokenIds" in information[0]["markets"][0]
                and isinstance(information[0]["markets"][0]["outcomes"], str)
                and isinstance(information[0]["markets"][0]["clobTokenIds"], str)

        )
        if not data_is_valid:
            return []

        list_of_instruments = information
        return list_of_instruments

    @staticmethod
    def get_instruments(config: PolymarketConfig) -> dict:
        rest_client = RestClient()
        msg = {
            "limit": 200,
            "ascending": "true",
            "closed": "false",
            # this is correlated with the "btc-up-or-down-5m"
            "series_id": config.series_id
        }

        data = rest_client.get_request(full_url=config.base_url, msg=msg)

        events = []
        for i in range(10):
            # we are assuming there will not be more than 10 pages of instruments to find thus will quit if 10 pages or more are present
            # when the next_cursor goes we can still add events together
            if "events" in data:
                events += data['events']

            if "next_cursor" not in data: break

            time.sleep(1)
            # pagination occurs when we use the next_cursor to fill in the parameter for after_cursor -> it allows you to move on to the next set of pages .....
            params = {
                'after_cursor': data.get("next_cursor"),
                "limit": 200,
                "ascending": "true",
                "closed": "false",
                # this is correlated with the "btc-up-or-down-5m"
                "series_id": config.series_id
            }

            data = rest_client.get_request(full_url=config.base_url, msg=params)

        return events

    def set_channels_in_msg(self):
        self.msg["assets_ids"] = self.channels


    # need to change this
    def get_unsubscribe_from_channel_msg(self, channels:list):
        return {
            "operation": "unsubscribe",
            "assets_ids": channels,
        }

    def get_subscribe_to_channel_msg(self, channels:list):
        return {
            "assets_ids": channels,
            "type": "market",
            "level": 2
        }

    def get_subscribe_to_additional_channel_msg(self, channels:list):
        return {
            "operation": "subscribe",
            "assets_ids": channels,
        }

    def get_authentication_info(self) -> None:
        return None

    def get_refresh_authentication_info(self) -> None:
        return None

    def validate_authentication(self, authentication_message) -> bool:
        valid: bool = ((isinstance(authentication_message, dict)
                        and "result" in authentication_message)
                       and "access_token" in authentication_message["result"]
                       and "refresh_token" in authentication_message["result"])

        if valid:
            self._access_token = authentication_message["result"]["access_token"]
            self._refresh_token = authentication_message["result"]["refresh_token"]
            self._token_expires_in = authentication_message["result"]["expires_in"]
            self._time_token_collected = time.time()

        return valid

    def validate_message(self, msg) -> bool:

        outcome = bool(msg)

        # is_standard_valid = bool(msg)
        #
        # if is_standard_valid:
        #     if isinstance(msg, list) and len(msg) > 0:
        #         outcome = (
        #                 isinstance(msg[0], dict)
        #                 and "bids" in msg[0]
        #                 and "asks" in msg[0]
        #                 and "timestamp" in msg[0]
        #                 and "tick_size" in msg[0]
        #                 and "event_type" in msg[0]
        #                 and "market" in msg[0]
        #                 and "asset_id" in msg[0]
        #         )
        #     elif isinstance(msg, dict) and "price_changes" in msg and isinstance(msg["price_changes"], list) and len(msg["price_changes"]) > 0:
        #         outcome = (
        #                 "asset_id" in msg['price_changes'][0]
        #                 and "price" in msg['price_changes'][0]
        #                 and "size" in msg['price_changes'][0]
        #                 and "side" in msg['price_changes'][0]
        #                 and "best_bid" in msg['price_changes'][0]
        #                 and "best_ask" in msg['price_changes'][0]
        #         )
        #     elif isinstance(msg, dict) and "event_type" in msg and msg["event_type"] == "book" and ("bids" in msg) and ("asks" in msg):
        #         outcome = (
        #                 isinstance(msg['bids'], list) and len(msg['bids']) > 0
        #                 and isinstance(msg['asks'], list) and len(msg['asks']) > 0
        #         )
        #     elif isinstance(msg, dict) and "event_type" in msg and msg["event_type"] == "last_trade_price" and ("price" in msg) and ("size" in msg):
        #         outcome = (msg['price'] is not None) and (msg['size'] is not None)
        #
        #     else:
        #         print(msg)

        return outcome

    def restructure_data(self, data) -> dict | list:

        if 'exch_ts_sec' in data:
            return data


        if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):

            if "event_type" not in data[0]:
                return []

            # we need this here as we are getting the time of hte event captured which is stored as a dict at the end of a list in the websocket client
            # Thus we need to get the LAST element from this list
            if "sys_time" in data[-1]:
                sys_time = data[-1]["sys_time"]
                sys_ts_sec = int(sys_time)
                sys_ts_micro = int((sys_time - sys_ts_sec) * 1_000_000)
                removed_item = data.pop(-1)
                for item in data:
                    PolymarketAdapter.add_new_formatted_timestamp(data_dict=item, timestamp_sys=sys_time,timestamp_exc=int(item["timestamp"]), sys_ts_sec=sys_ts_sec, sys_ts_micro=sys_ts_micro)
                    item['channel'] = item['asset_id']

                return data

        if isinstance(data, dict) and "event_type" in data:
            if "price_changes" in data:
                PolymarketAdapter.add_new_formatted_timestamp(data_dict=data, timestamp_sys=data["sys_time"], timestamp_exc=int(data["timestamp"]))
                price_changes_list:list = data.pop("price_changes")
                for key in data.keys():
                    for item in price_changes_list:
                        item[key] = data[key]

                for item in price_changes_list:
                    item["channel"] = item['asset_id']

                return price_changes_list

            elif (
                    ("book" in data["event_type"])
                    or ("last_trade_price" in data["event_type"])
                    or ("tick_size_change" in data["event_type"])
            ):
                # check these events work with the events below in the streampipline -> as we converting them to lists and saving them that way
                PolymarketAdapter.add_new_formatted_timestamp(data_dict=data, timestamp_sys=data["sys_time"],timestamp_exc=int(data["timestamp"]))
                data['channel'] = data['asset_id']

                return data
            # remove this if we catch nothing
            else:
                print(data)
        #     remove this if we catch nothing over a day

        # we have gone through all filtering if we can't sort it assume it is failed data
        return []

    # getting a dict and passing new formatted datetime stamps into the object
    @staticmethod
    def add_new_formatted_timestamp(data_dict:dict, timestamp_sys:int, timestamp_exc:int, sys_ts_sec=None, sys_ts_micro=None):
        if sys_ts_sec is None:
            sys_ts_sec = int(timestamp_sys)
            sys_ts_micro = int((timestamp_sys - sys_ts_sec) * 1_000_000)

        temp = (timestamp_exc / 1000)
        exch_ts_sec = int(temp)
        exch_ts_micro = int((temp - exch_ts_sec) * 1_000_000)

        data_dict['exch_ts_sec'] = exch_ts_sec
        data_dict['exch_ts_micro'] = exch_ts_micro
        data_dict['sys_ts_sec'] = sys_ts_sec
        data_dict['sys_ts_micro'] = sys_ts_micro

    def write_data_file(self, path_to_file:str, info):
        for i in range(3):
            try:
                # compresses files to make sure the files don't take up too much space
                with gzip.open(path_to_file, 'wt', encoding="utf-8", compresslevel=9) as f:
                    data = json.dumps(info, separators=(",", ":")) + "\n"
                    f.write(data)

                return
            except Exception as e:
                print(e, "could not save meta data file for ", path_to_file)

        print(f"failed to save meta data for {path_to_file} giving up")


    @override
    def writer(self, normalised_list_of_data:list) -> None:
        # try to write 3 times before giving up
        for i in range(3):
            try:
                channel = normalised_list_of_data[0]['asset_id']

                event_info = PolymarketAdapter.event_info.get(channel)
                if event_info is None:
                    raise ValueError("event_info is none in writer thus, key missing")


                clobTokenIds = event_info["markets"][0]['clobTokenIds']
                outcomes = event_info["markets"][0]['outcomes']

                full_single_channel = None
                for i in range(len(outcomes) ):
                    if channel == clobTokenIds[i]:
                        full_single_channel = f"{event_info['ticker']}_{outcomes[i]}_{channel}"
                        break

                if full_single_channel is None:
                    raise ValueError("full_single_channel is None and could not form a string to save")

                naming_parts = full_single_channel.split("_")

                filename = f"data_{self.exchange_name}_{full_single_channel}_{int(time.time())}.jsonl.gz"
                # the path package finds the folder at the highest level that is the same data -it all relative
                meta_data_dir =  (self.PATH_DIR / self.exchange_name/ naming_parts[0] / naming_parts[1] )
                meta_data_dir.mkdir(parents=True, exist_ok=True)
                meta_data_dir_to_file = (meta_data_dir / "meta_data.json.gz").resolve()
                if not os.path.exists(meta_data_dir_to_file):
                    # create meta data file
                    self.write_data_file(path_to_file=meta_data_dir_to_file, info=event_info)

                new_dir = (self.PATH_DIR / self.exchange_name/ naming_parts[0] / naming_parts[1] / naming_parts[2] )
                new_dir.mkdir(parents=True, exist_ok=True)
                dir_to_file = new_dir / filename
                self.write_data_file(path_to_file=dir_to_file.resolve(), info=normalised_list_of_data)
                # exit for loop
                return
            except Exception as e:
                print(time.time(),"failed to write: ", e)
                # in another thread so sleeping will not affect the thread
                time.sleep(1)