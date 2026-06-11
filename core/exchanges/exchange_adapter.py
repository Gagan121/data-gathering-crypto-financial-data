# nearest thing to abstract classes
from abc import ABC, abstractmethod
import pandas as pd
import os
import time

# helper function
def create_base_folder(exchange_name:str, path_to_base_folder:str) -> str:
    new_base_path_for_exchange = os.path.join(path_to_base_folder, exchange_name)
    # making folder if one isn't already there, most paths will be the same area
    if not os.path.exists(new_base_path_for_exchange):
        os.makedirs(new_base_path_for_exchange)
    return new_base_path_for_exchange

class ExchangeAdapter(ABC):
    def __init__(self, path_to_folder:str, exchange_name:str, url:str, msg:dict, ticker:str) -> None:
        self.normalised_list_of_data = []
        self.path_to_folder = path_to_folder
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

    def get_url(self) -> str:
        return self.url

    def get_msg(self):
        return self.msg

    def normalise_data(self, batch_list:list) -> list:
        normalised_data = []
        for data in batch_list:
            norm_data = self.get_structure_of_data(data)
            normalised_data.append(norm_data)
        return normalised_data


    def check_for_duplicates(self, msg) -> bool:
        data = self.get_structure_of_data(msg)

        # we do not need to keep track of bid/ask quantity changes -> as new info comes in if there is a size change in the quotes
        if (self.previous_ask_bid_value['bid'] == data['bid']) and (self.previous_ask_bid_value['ask'] == data['ask']):
            return True

        self.previous_ask_bid_value['bid'] = data['bid']
        self.previous_ask_bid_value['ask'] = data['ask']
        return False

    def writer(self, normalised_list_of_data):
        # try to write 3 times before giving up
        for i in range(3):
            try:
                df = pd.DataFrame(normalised_list_of_data)

                filename = f"data_{self.exchange_name}_{self.ticker}_{int(time.time())}.parquet"

                path_to_new_base_folder = create_base_folder(self.exchange_name, self.path_to_folder)
                full_path_to_file = os.path.join(path_to_new_base_folder, filename)
                df.to_parquet(path=full_path_to_file)
                # exit for loop
                return
            except Exception as e:
                print("failed to write: ", e)
                # in another thread so sleeping will not affect the thread
                time.sleep(1)