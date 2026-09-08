from __future__ import annotations

from abc import abstractmethod, ABC
from typing import TypeVar, Generic
from core.exchanges.exchange_adapter import ExchangeAdapter

from dataclasses import dataclass


@dataclass
class ExchangeConfig(ABC):
    limit_number_of_channels: int
    interval_type: str
    base_url: str
    msg: dict
    websocket_url: str
    exchange_name: str

    @abstractmethod
    def get_exchange_adapter_type(self) -> type:
        pass

class ExchangeWithExpiry(ExchangeAdapter):

    def __init__(self, base_url: str, exchange_info: dict, channels: list, exchange_name: str, websocket_url: str,
                 msg: dict, ticker: str, heart_beat_msg: dict | None = None,
                 heart_beat_reply_msg: dict | None = None) -> None:

        super().__init__(channels=channels, exchange_name=exchange_name, websocket_url=websocket_url, msg=msg, ticker=ticker, heart_beat_msg=heart_beat_msg, heart_beat_reply_msg=heart_beat_reply_msg)
        self.base_url = base_url
        self.exchange_info = exchange_info

    @staticmethod
    @abstractmethod
    def get_instruments(config: ExchangeConfig) -> dict:
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

    @staticmethod
    @abstractmethod
    def create_new_adapter(channels:list, config:ExchangeConfig) -> ExchangeWithExpiry:
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