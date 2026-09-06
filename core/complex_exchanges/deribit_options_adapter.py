import time
from datetime import datetime
from decimal import Decimal
from core.exchanges.exchange_adapter import flatten, convert_to_decimal_and_quantize
from core.complex_exchanges.exchanges_with_expiry import ExchangeWithExpiry
from core.rest_requests.rest_client_requests import RestClient
import os
from dotenv import load_dotenv
from urllib.parse import urljoin
import copy
from dataclasses import dataclass, fields

load_dotenv()

@dataclass
class DeribitOptionsConfig:
    limit_number_of_channels: int
    interval_type: str
    base_url: str
    msg: dict
    currency: str
    expired: str
    data_types: list
    websocket_url: str
    exchange_name: str
    heart_beat_msg: dict
    heart_beat_reply_msg: dict

    def __post_init__(self):

        for field in fields(self):
            if getattr(self, field.name) is None:
                raise ValueError(f"{field.name} cannot be None")

        if self.limit_number_of_channels <= 0:
            raise ValueError("limit_number_of_channels must be greater than 0")

        if self.limit_number_of_channels >= 500:
            raise ValueError("limit_number_of_channels must be smaller 500")

        if self.currency not in ("BTC", "ETH"):
            raise ValueError("currency must be BTC or ETH")

        if self.interval_type not in ("agg2", "raw", "100ms"):
            raise ValueError("invalid interval_type")



#         check all values are not None


class DeribitOptionsAdapter(ExchangeWithExpiry[DeribitOptionsConfig]):

    # used to get the number of instruments we are looking for
    def __init__(self, base_url:str, exchange_info:dict, channels: list, exchange_name: str, websocket_url: str, msg: dict, ticker: str,
                 heart_beat_msg: dict, heart_beat_reply_msg:dict) -> None:

        super().__init__(base_url=base_url, exchange_info=exchange_info, channels=channels, exchange_name=exchange_name, websocket_url=websocket_url, msg=msg, ticker=ticker, heart_beat_msg=heart_beat_msg, heart_beat_reply_msg=heart_beat_reply_msg)
        load_dotenv()

        self._client_id = os.getenv("DERIBIT_PERPETUAL_CLIENT_ID")
        self._client_secret = os.getenv("DERIBIT_PERPETUAL_CLIENT_SECRET")

    def create_new_adapter(self, channels:list):
        return type(self)(
            base_url=self.base_url,
            exchange_info=copy.deepcopy(self.exchange_info),
            channels=channels,
            exchange_name=self.exchange_name,
            websocket_url=self.websocket_url,
            msg=copy.deepcopy(self.msg),
            ticker=self.ticker,
            heart_beat_msg=copy.deepcopy(self.heart_beat_msg),
            heart_beat_reply_msg=copy.deepcopy(self.heart_beat_reply_msg)
        )



    def add_request_id(self, msg:dict, request_id) -> dict:
        if (msg is not None) and isinstance(msg, dict):
            msg["id"] = request_id

        return msg


    @staticmethod
    def sort_data_form_new_requests(information) -> list:

        data_is_valid = (
                isinstance(information, dict)
                and ("result" in information)
                and isinstance(information["result"], list)
                and len(information["result"]) > 0
        )
        if not data_is_valid:
            return []

        list_of_instruments = information["result"]
        return list_of_instruments

    @staticmethod
    def get_instruments(config: DeribitOptionsConfig) -> dict:
        rest_client = RestClient()
        msg = {
            "method": "public/get_instruments",
            "params": {
                "currency": config.currency,
                "kind": "option",
                "expired": config.expired,
            }
        }

        full_url = urljoin(config.base_url, msg['method'])
        # you have to break the request up into the params as it give you everything unfiltered
        exchange_info_on_instruments = rest_client.get_request(full_url=full_url, msg=msg['params'])

        return exchange_info_on_instruments

    def set_channels_in_msg(self):
        self.msg["params"]['channels'] = self.channels


    def get_unsubscribe_from_channel_msg(self, channels:list):
        return {
            "jsonrpc": "2.0",
            "method": "private/unsubscribe",
            "params": {
                "channels": channels
            }
        }

    def get_subscribe_to_channel_msg(self, channels:list):
        return {
            "jsonrpc": "2.0",
            "method": "private/subscribe",
            # "method": "public/subscribe",
            "params": {
                "channels": channels
            }
        }

    def get_authentication_info(self) -> dict:
        return {
            "jsonrpc": "2.0",
            "method": "public/auth",
            "params": {
                "grant_type": "client_credentials",
                "client_id": self._client_id,
                "client_secret": self._client_secret
            }
        }

    def get_refresh_authentication_info(self) -> dict:
        return {
            "jsonrpc": "2.0",
            "method": "public/auth",
            "params": {
                "grant_type": "refresh_token",
                "refresh_token": self._refresh_token
            }
        }

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

        outcome = False

        is_standard_valid = (isinstance(msg, dict)
                             and "params" in msg
                             and "data" in msg["params"])

        if is_standard_valid:
            if "ticker" in msg['params']['channel']:
                outcome = (
                        isinstance(msg, dict)
                        and "params" in msg
                        and "data" in msg["params"]
                        and "timestamp" in msg["params"]["data"]
                        and "stats" in msg["params"]["data"]
                        and "best_bid_price" in msg["params"]["data"]
                        and "best_ask_price" in msg["params"]["data"]
                        and "best_bid_amount" in msg["params"]["data"]
                        and "best_ask_amount" in msg["params"]["data"]
                )
            elif "trades" in msg['params']['channel']:
                outcome = (
                        isinstance(msg, dict)
                        and "params" in msg
                        and "data" in msg["params"]
                        and isinstance(msg["params"]['data'], list)
                )

        return outcome

    def restructure_data(self, data) -> dict | list:

        if 'exch_ts_sec' in data:
            return data

        if "ticker" in data['params']['channel']:
            return self.restructure_ticker_data(data)
        elif "trades" in data['params']['channel']:
            return self.restructure_trade_data(data)

        return dict()

    def restructure_ticker_data(self, data) -> dict:
        ts = data['params']['data']['timestamp']
        sys_time = data['sys_time']
        temp = (ts / 1000)
        exch_ts_sec = int(temp)
        exch_ts_micro = int((temp - exch_ts_sec) * 1_000_000)

        sys_ts_sec = int(sys_time)
        sys_ts_micro = int((sys_time - sys_ts_sec) * 1_000_000)

        new_data = flatten(data["params"]["data"])
        # removing the keys from the dictionary
        new_data.pop('state', None)
        new_data.pop('instrument_name', None)

        try:
            return {
                "channel" : data["params"]["channel"],
                'exch_ts_sec': exch_ts_sec,
                'exch_ts_micro': exch_ts_micro,
                'sys_ts_sec': sys_ts_sec,
                'sys_ts_micro': sys_ts_micro,
                'underlying_index': new_data.get('underlying_index'),
                'underlying_price': convert_to_decimal_and_quantize(new_data.get('underlying_price')),
                'bid': convert_to_decimal_and_quantize(new_data.get('best_bid_price')),
                'ask': convert_to_decimal_and_quantize(new_data.get('best_ask_price')),
                'bid_quantity': convert_to_decimal_and_quantize(new_data.get('best_bid_amount')),
                'ask_quantity': convert_to_decimal_and_quantize(new_data.get('best_ask_amount')),
                'high': convert_to_decimal_and_quantize(new_data.get('high')),
                'low': convert_to_decimal_and_quantize(new_data.get('low')),
                'price_change': convert_to_decimal_and_quantize(new_data.get('price_change')),
                'volume': convert_to_decimal_and_quantize(new_data.get('volume')),
                'volume_usd': convert_to_decimal_and_quantize(new_data.get('volume_usd')),
                'delta': convert_to_decimal_and_quantize(new_data.get('delta')),
                'gamma': convert_to_decimal_and_quantize(new_data.get('gamma')),
                'vega': convert_to_decimal_and_quantize(new_data.get('vega')),
                'theta': convert_to_decimal_and_quantize(new_data.get('theta')),
                'rho': convert_to_decimal_and_quantize(new_data.get('rho')),
                'index_price': convert_to_decimal_and_quantize(new_data.get('index_price')),
                'last_price': convert_to_decimal_and_quantize(new_data.get('last_price')),
                'settlement_price': convert_to_decimal_and_quantize(new_data.get('settlement_price')),
                'min_price': convert_to_decimal_and_quantize(new_data.get('min_price')),
                'max_price': convert_to_decimal_and_quantize(new_data.get('max_price')),
                'open_interest': convert_to_decimal_and_quantize(new_data.get('open_interest')),
                'mark_price': convert_to_decimal_and_quantize(new_data.get('mark_price')),
                'interest_rate': convert_to_decimal_and_quantize(new_data.get('interest_rate')),
                'estimated_delivery_price': convert_to_decimal_and_quantize(new_data.get('estimated_delivery_price')),
                'mark_iv': convert_to_decimal_and_quantize(new_data.get('mark_iv')),
                'bid_iv': convert_to_decimal_and_quantize(new_data.get('bid_iv')),
                'ask_iv': convert_to_decimal_and_quantize(new_data.get('ask_iv')),
            }
        except (KeyError, IndexError, TypeError):
            return dict()

    def restructure_trade_data(self, data) -> list:
        if not isinstance(data['params']['data'], list):
            raise ValueError("malformed data type, not list for trades data formatting")

        sys_time = data['sys_time']
        sys_ts_sec = int(sys_time)
        sys_ts_micro = int((sys_time - sys_ts_sec) * 1_000_000)
        trade_list = []
        for i in range(len(data['params']['data'])):
            trade = data['params']['data'][i]
            ts = trade.get('timestamp')
            temp = (ts / 1000)
            exch_ts_sec = int(temp)
            exch_ts_micro = int((temp - exch_ts_sec) * 1_000_000)

            try:

                trade = {
                    "channel": data['params']['channel'],
                    'sys_ts_sec': sys_ts_sec,
                    'sys_ts_micro': sys_ts_micro,
                    'exch_ts_sec': exch_ts_sec,
                    'exch_ts_micro': exch_ts_micro,
                    'price': convert_to_decimal_and_quantize(trade.get('price')),
                    'iv': convert_to_decimal_and_quantize(trade.get('iv')),
                    'direction': trade.get('direction'),
                    'index_price': convert_to_decimal_and_quantize(trade.get('index_price')),
                    'instrument_name': trade.get('instrument_name'),
                    'trade_seq': trade.get('trade_seq'),
                    'amount': convert_to_decimal_and_quantize(trade.get('amount')),
                    'mark_price': convert_to_decimal_and_quantize(trade.get('mark_price')),
                    'tick_direction': int(trade.get('tick_direction')),
                    'starbase_match_id': convert_to_decimal_and_quantize(trade.get('starbase_match_id')),
                    'trade_id': convert_to_decimal_and_quantize(trade.get('trade_id')),
                    'contracts': convert_to_decimal_and_quantize(trade.get('contracts')),
                    'starbase_timestamp': convert_to_decimal_and_quantize(trade.get('starbase_timestamp')),
                }
            except (KeyError, IndexError, TypeError):
                continue

            trade_list.append(trade)

        return trade_list
