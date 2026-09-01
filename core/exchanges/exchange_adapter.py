# nearest thing to abstract classes
from abc import ABC, abstractmethod
import pandas as pd
import os
import time
from pathlib import Path
from decimal import Decimal
import re

# may not need this flattern object if, we make this raw-> high,low..... -> as they would not exist
# help flatten complex nested dictionaries
def flatten(data):
    result = {}
    for key, value in data.items():
        if isinstance(value, dict):
            result.update(flatten(value))
        else:
            result[key] = value

    return result

def convert_to_decimal_and_quantize(value):
    try:
        # if the value is none then put 0 otherwise use the original value
        if value is None:
            return_value = Decimal(0)
        elif isinstance(value, float):
            return_value = Decimal(value).quantize(Decimal("0.0000001"))
        elif isinstance(value, int):
            return_value = value
        else:
            return_value = Decimal(value)

    except Exception as e:
        print(e, " converting value to Decimal ", value )
        return_value = value

    return return_value


class ExchangeAdapter(ABC):
    def __init__(self, channels:list, exchange_name:str, url:str, msg:dict, ticker:str, heart_beat_msg:dict|None = None, heart_beat_reply_msg:dict|None = None ) -> None:
        self.normalised_list_of_data = []
        self.channels = channels
        # if data doesn't exist path()mkdir will create the whole directory path including the parent
        self.PATH_DIR = Path("data")
        print("new_dir:", self.PATH_DIR)
        print("new_dir absolute:", self.PATH_DIR.resolve())
        self.exchange_name = exchange_name
        self.url = url
        self.msg = msg
        self.heart_beat_msg = heart_beat_msg
        self.heart_beat_reply_msg = heart_beat_reply_msg
        self.ticker = ticker
        self.previous_ask_bid_value = {
            "bid": 0,
            "ask": 0,
            "bid_quantity": 0,
            "ask_quantity": 0
        }

        self._access_token = ""
        self._refresh_token = ""
        self._token_expires_in = 0
        self._time_token_collected = 0

    @abstractmethod
    def restructure_data(self, data) -> dict|list:
        pass

    @abstractmethod
    def validate_message(self, msg) -> bool:
        pass

    @abstractmethod
    def get_authentication_info(self) -> dict|None:
        pass

    @abstractmethod
    def get_refresh_authentication_info(self) -> dict:
        pass

    @abstractmethod
    def validate_authentication(self, authentication_message) -> bool:
        pass

    def get_channels(self) -> list:
        return self.channels

    def get_msg_request(self) -> dict:
        return self.msg

    def clear_tokens(self):
        self._access_token = ""
        self._refresh_token = ""

    def if_refresh_token_exists(self) -> bool:
        return self._refresh_token != ""

    def get_time_token_expires(self) -> int:
        expiry_time = 0
        try:
            expiry_time = int(self._token_expires_in)
        except ValueError as e:
            print(e, "casting issue occurred")

        return expiry_time

    def get_time_token_collected(self) -> float:
        return self._time_token_collected

    def check_if_authentication_exist(self) -> bool:
        return self.get_authentication_info() is None

    def valid_message_can_pass_and_restructure_data(self, msg) -> dict:
        # bool_2 = False
        bool_1 = self.validate_message(msg)
        if bool_1:
            # if data doesn't have the right keys
            data = self.restructure_data(msg)
            # encase our data is empty then
            bool_2 = bool(data)
            # data is saved on the exchange adapter for the time being as its a large piece of data, and remaking is a waste of time
            # dict means tickers, list means trades
            """
            # if we ignore the duplication and just see what comes in for now we can see if any patterns come up
            # if isinstance(data, dict):
            #     bool_2 = self.check_quotes_diff(data)
            # else:
            #     bool_2 = True
            """

            return {
                "valid" : (bool_1 and bool_2),
                "data" : data
            }

        return {"valid" : False, "data" : dict()}

    def get_url(self) -> str:
        return self.url

    def get_data_request_msg(self):
        return self.msg

    def get_heart_beat_msg(self):
        return self.heart_beat_msg

    def get_heart_beat_reply_msg(self):
        return self.heart_beat_reply_msg

    def normalise_data(self, batch_list:list) -> list:
        normalised_data = []
        for data in batch_list:
            norm_data = self.restructure_data(data)
            # only add dictionary if there is data in it -> if empty output is false
            if bool(norm_data):
                normalised_data.append(norm_data)
        return normalised_data


    def check_quotes_diff(self, data) -> bool:
        try:
            # we do not need to keep track of bid/ask quantity changes -> as new info comes in if there is a size change in the quotes
            if ((self.previous_ask_bid_value['bid'] != data['bid'])
                    or (self.previous_ask_bid_value['ask'] != data['ask'])
                    or (self.previous_ask_bid_value['ask_quantity'] != data['ask_quantity'])
                    or (self.previous_ask_bid_value['bid_quantity'] != data['bid_quantity'])
            ):
                self.previous_ask_bid_value['bid'] = data['bid']
                self.previous_ask_bid_value['ask'] = data['ask']
                self.previous_ask_bid_value['bid_quantity'] = data['bid_quantity']
                self.previous_ask_bid_value['ask_quantity'] = data['ask_quantity']
                return True
        except (KeyError, IndexError, TypeError):
            pass

        return False

    def writer(self, normalised_list_of_data:list) -> None:
        # try to write 3 times before giving up
        for i in range(3):
            try:
                df = pd.DataFrame(normalised_list_of_data)
                data = normalised_list_of_data[-1]["channel"]
                # convert all
                name_of_data_set = re.sub(r"[.-]", "_", data)
                # remove duplicate ticker name
                name_of_data_set = re.sub(self.ticker, "", name_of_data_set)
                # remove trailing characters in front and behind
                name_of_data_set = re.sub(r'^[^a-zA-Z0-9._-]+|[^a-zA-Z0-9._-]+$', '', name_of_data_set)

                name_of_data_set = f"{self.ticker}_{name_of_data_set}"

                name_of_data_set = re.sub("__", "_", name_of_data_set)

                filename = f"data_{self.exchange_name}_{name_of_data_set}_{int(time.time())}.parquet"
                # the path package finds the folder at the highest level that is the same data -it all relative
                new_dir = (self.PATH_DIR / self.exchange_name/ name_of_data_set )
                new_dir.mkdir(parents=True, exist_ok=True)
                dir_to_file = new_dir / filename
                df.to_parquet(path=dir_to_file.resolve())
                # exit for loop
                return
            except Exception as e:
                print(time.time(),"failed to write: ", e)
                # in another thread so sleeping will not affect the thread
                time.sleep(1)