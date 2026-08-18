# nearest thing to abstract classes
from abc import ABC, abstractmethod
import pandas as pd
import os
import time
from pathlib import Path

class ExchangeAdapter(ABC):
    def __init__(self, exchange_name:str, url:str, msg:dict, ticker:str) -> None:
        self.normalised_list_of_data = []
        self.PATH_DIR = Path("data")
        print("new_dir:", self.PATH_DIR)
        print("new_dir absolute:", self.PATH_DIR.resolve())
        self.exchange_name = exchange_name
        self.url = url
        self.msg = msg
        self.ticker = ticker
        self.previous_ask_bid_value = {
            "bid": 0,
            "ask": 0
        }

    @abstractmethod
    def get_structure_of_data(self, data) -> dict:
        pass

    @abstractmethod
    def validate_message(self, msg) -> bool:
        pass

    def valid_message_can_pass(self, msg) -> bool:
        return self.validate_message(msg) and self.check_quotes_diff(msg)

    def get_url(self) -> str:
        return self.url

    def get_msg(self):
        return self.msg

    def normalise_data(self, batch_list:list) -> list:
        normalised_data = []
        for data in batch_list:
            norm_data = self.get_structure_of_data(data)
            # only add dictionary if there is data in it -> if empty output is false
            if bool(norm_data):
                normalised_data.append(norm_data)
        return normalised_data


    def check_quotes_diff(self, msg) -> bool:
        try:
            # if data doesn't have the right keys
            data = self.get_structure_of_data(msg)

            # we do not need to keep track of bid/ask quantity changes -> as new info comes in if there is a size change in the quotes
            if (self.previous_ask_bid_value['bid'] != data['bid']) or (self.previous_ask_bid_value['ask'] != data['ask']):
                self.previous_ask_bid_value['bid'] = data['bid']
                self.previous_ask_bid_value['ask'] = data['ask']
                return True
        except (KeyError, IndexError, TypeError):
            pass

        return False

    def writer(self, normalised_list_of_data):
        # try to write 3 times before giving up
        for i in range(3):
            try:
                df = pd.DataFrame(normalised_list_of_data)

                filename = f"data_{self.exchange_name}_{self.ticker}_{int(time.time())}.parquet"
                # the path package finds the folder at the highest level that is the same data -it all relative
                new_dir = (self.PATH_DIR / self.exchange_name)
                new_dir.mkdir(parents=True, exist_ok=True)
                dir_to_file = new_dir / filename
                df.to_parquet(path=dir_to_file.resolve())
                # exit for loop
                return
            except Exception as e:
                print("failed to write: ", e)
                # in another thread so sleeping will not affect the thread
                time.sleep(1)