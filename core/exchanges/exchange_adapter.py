# nearest thing to abstract classes
from abc import ABC, abstractmethod
import pandas as pd
import os

# helper function
def create_base_folder(exchange_name:str, path_to_base_folder:str) -> str:
    new_base_path_for_exchange = os.path.join(path_to_base_folder, exchange_name)
    # making folder if one isn't already there, most paths will be the same area
    if not os.path.exists(new_base_path_for_exchange):
        os.makedirs(new_base_path_for_exchange)
    return new_base_path_for_exchange

class ExchangeAdapter(ABC):
    def __init__(self, path_to_folder:str, exchange_name:str):
        self.normalised_list_of_data = []
        self.path_to_folder = path_to_folder
        self.exchange_name = exchange_name

    @abstractmethod
    def normalise_data(self, data_lot: list) -> list:
        pass

    @abstractmethod
    def validate_message(self, msg):
        pass


    def writer(self, normalised_list_of_data: list, filename: str):
        df = pd.DataFrame(normalised_list_of_data)
        path_to_new_base_folder = create_base_folder(self.exchange_name, self.path_to_folder)
        full_path_to_file = os.path.join(path_to_new_base_folder, filename)
        df.to_parquet(path=full_path_to_file)