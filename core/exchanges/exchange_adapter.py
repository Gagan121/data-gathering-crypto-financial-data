import asyncio
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
    def __init__(self, path_to_folder:str, exchange_name:str, url:str, msg:dict) -> None:
        self.normalised_list_of_data = []
        self.path_to_folder = path_to_folder
        self.exchange_name = exchange_name
        self.url = url
        self.msg = msg
        # self.batch_list = []
        # self.max_size_for_queue = 1000
        #
        # self.queue = asyncio.Queue(maxsize=self.max_size_for_queue)
        # self.normalised_list_of_data = []



    # def check_batch_size_reach_max(self) -> bool:
    #     return len(self.batch_list) >= (self.max_size_for_queue -1)
    #
    # def get_max_size_for_queue(self):
    #     return self.max_size_for_queue
    #
    #
    # async def add_to_queue(self, msg):
    #     await self.queue.put(msg)
    #
    # async def get_item_from_queue(self):
    #     return await self.queue.get()
    #
    # def complete_queue_task(self):
    #     self.queue.task_done()
    #
    #
    # def add_to_batch_list(self, msg):
    #     self.batch_list.append(msg)
    #
    # def get_batch_list(self):
    #     return self.batch_list
    #
    # def clear_batch_list(self):
    #     self.batch_list = []

    def get_url(self) -> str:
        return self.url

    def get_msg(self):
        return self.msg

    @abstractmethod
    def validate_message(self, msg):
        pass

    @abstractmethod
    def normalise_data(self, batch_list:list) -> list:
        pass

    def writer(self, normalised_list_of_data):
        # try to write 3 times before giving up
        for i in range(3):
            try:
                df = pd.DataFrame(normalised_list_of_data)

                filename = f"data_{self.exchange_name}_{int(time.time())}.parquet"

                path_to_new_base_folder = create_base_folder(self.exchange_name, self.path_to_folder)
                full_path_to_file = os.path.join(path_to_new_base_folder, filename)
                df.to_parquet(path=full_path_to_file)
                # exit for loop
                return
            except Exception as e:
                print("failed to write: ", e)