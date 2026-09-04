from abc import abstractmethod
from typing import TypeVar, Generic

from core.exchanges.exchange_adapter import ExchangeAdapter

T = TypeVar("T")

class ExchangeWithExpiry(ExchangeAdapter, Generic[T]):

    def __init__(self, base_url: str, exchange_info: dict, channels: list, exchange_name: str, websocket_url: str,
                 msg: dict, ticker: str, heart_beat_msg: dict | None = None,
                 heart_beat_reply_msg: dict | None = None) -> None:

        super().__init__(channels=channels, exchange_name=exchange_name, websocket_url=websocket_url, msg=msg, ticker=ticker, heart_beat_msg=heart_beat_msg, heart_beat_reply_msg=heart_beat_reply_msg)
        self.base_url = base_url
        self.exchange_info = exchange_info


    @staticmethod
    @abstractmethod
    def get_instruments(config: T) -> dict:
        pass

    @staticmethod
    @abstractmethod
    def sort_data_form_new_requests(information) -> list:
        pass

    def get_base_url(self) -> str:
        return self.base_url

    def get_exchange_info(self) -> dict:
        return self.exchange_info

    def set_channels(self, channels: list) -> None:
        self.channels = channels

    @abstractmethod
    def create_new_adapter(self, channels:list):
        pass

    @abstractmethod
    def set_channels_in_msg(self):
        pass

    @abstractmethod
    def get_unsubscribe_from_channel_msg(self, channels:list):
        pass

    @abstractmethod
    def get_subscribe_to_channel_msg(self, channels:list):
        pass