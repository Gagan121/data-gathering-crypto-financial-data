import time
from datetime import datetime
from decimal import Decimal
from core.exchanges.exchange_adapter import ExchangeAdapter, flatten, convert_to_decimal_and_quantize
from core.rest_requests.rest_client_requests import RestClient
import os
from dotenv import load_dotenv
from urllib.parse import urljoin
import copy

load_dotenv()


class DeribitOptionsAdapter(ExchangeAdapter):
    # used to get the number of instruments we are looking for
    def __init__(self, channels: list, exchange_name: str, websocket_url: str, msg: dict, ticker: str,
                 heart_beat_msg: dict, heart_beat_reply_msg=dict) -> None:

        super().__init__(channels=channels, exchange_name=exchange_name, url=websocket_url, msg=msg, ticker=ticker,
                         heart_beat_msg=heart_beat_msg, heart_beat_reply_msg=heart_beat_reply_msg)
        load_dotenv()

        self._client_id = os.getenv("DERIBIT_PERPERTUAL_CLIENT_ID")
        self._client_secret = os.getenv("DERIBIT_PERPETUAL_CLIENT_SECRET")

    @classmethod
    def generate_deribit_option_adapters(cls, interval_type: str, base_url: str, msg: dict, currency: str, expired: str,
                                         data_types: list, websocket_url: str, exchange_name: str, heart_beat_msg: dict,
                                         heart_beat_reply_msg: dict) -> list:
        '''

        :param currency:
        :param expired:
        :param data_types: -> list of types for example ticker, trades,.....
        :param websocket_url:
        :param exchange_name:
        :param heart_beat_msg:
        :param heart_beat_reply_msg:
        '''
        data = {
            'currency': currency,
            'expired': expired,
        }

        rest_client = RestClient()
        information = DeribitOptionsAdapter.get_instruments(rest_client=rest_client, base_url=base_url, data=data)
        # true if information is there
        if not (bool(information)):
            return []
        list_of_instruments = DeribitOptionsAdapter.sort_data_form_new_requests(information)

        total_channels = []
        for item in list_of_instruments:
            for types_of_data in data_types:
                total_channels.append(f"{types_of_data}.{item['instrument_name']}.{interval_type}")

        list_of_lists_of_channels = [total_channels[x:x + 499] for x in range(0, len(total_channels), 499)]
        list_of_adapters = []
        for i in range(len(list_of_lists_of_channels)):
            # we have to create a copy here otherwise pass by reference would make all the msg the same
            adapter_msg = copy.deepcopy(msg)
            adapter_msg["params"]['channels'] = list_of_lists_of_channels[i]
            list_of_adapters.append(
                DeribitOptionsAdapter(
                    channels=list_of_lists_of_channels[i],
                    exchange_name=exchange_name,
                    websocket_url=websocket_url,
                    msg=adapter_msg,
                    ticker=currency,
                    heart_beat_msg=heart_beat_msg,
                    heart_beat_reply_msg=heart_beat_reply_msg
                )
            )

        return list_of_adapters

    @staticmethod
    def sort_data_form_new_requests(information) -> list | None:

        data_is_valid = (
                isinstance(information, dict)
                and ("result" in information)
                and isinstance(information["result"], list)
                and len(information["result"]) > 0
        )
        if not data_is_valid:
            return None

        list_of_instruments = information["result"]
        return list_of_instruments

    @staticmethod
    def get_instruments(rest_client, base_url, data) -> dict:
        msg = {
            "method": "public/get_instruments",
            "params": {
                "currency": data["currency"],
                "kind": "option",
                "expired": data["expired"],
            }
        }

        full_url = urljoin(base_url, msg['method'])
        # you have to break the request up into the params as it give you everything unfiltered
        data = rest_client.get_request(full_url=full_url, msg=msg['params'])

        return data

    def get_authentication_info(self) -> dict:
        return {
            "jsonrpc": "2.0",
            "id": 9929,
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
            "id": 9931,
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
                "channel": data['params']['channel'],
                'exch_ts_sec': exch_ts_sec,
                'exch_ts_micro': exch_ts_micro,
                'sys_ts_sec': sys_ts_sec,
                'sys_ts_micro': sys_ts_micro,
                'underlying_index': new_data['underlying_index'],
                'underlying_price': convert_to_decimal_and_quantize(new_data['underlying_price']),
                'bid': convert_to_decimal_and_quantize(new_data['best_bid_price']),
                'ask': convert_to_decimal_and_quantize(new_data['best_ask_price']),
                'bid_quantity': convert_to_decimal_and_quantize(new_data['best_bid_amount']),
                'ask_quantity': convert_to_decimal_and_quantize(new_data['best_ask_amount']),
                'high': convert_to_decimal_and_quantize(new_data['high']),
                'low': convert_to_decimal_and_quantize(new_data['low']),
                'price_change': convert_to_decimal_and_quantize(new_data['price_change']),
                'volume': convert_to_decimal_and_quantize(new_data['volume']),
                'volume_usd': convert_to_decimal_and_quantize(new_data['volume_usd']),
                'delta': convert_to_decimal_and_quantize(new_data['delta']),
                'gamma': convert_to_decimal_and_quantize(new_data['gamma']),
                'vega': convert_to_decimal_and_quantize(new_data['vega']),
                'theta': convert_to_decimal_and_quantize(new_data['theta']),
                'rho': convert_to_decimal_and_quantize(new_data['rho']),
                'index_price': convert_to_decimal_and_quantize(new_data['index_price']),
                'last_price': convert_to_decimal_and_quantize(new_data['last_price']),
                'settlement_price': convert_to_decimal_and_quantize(new_data['settlement_price']),
                'min_price': convert_to_decimal_and_quantize(new_data['min_price']),
                'max_price': convert_to_decimal_and_quantize(new_data['max_price']),
                'open_interest': convert_to_decimal_and_quantize(new_data['open_interest']),
                'mark_price': convert_to_decimal_and_quantize(new_data['mark_price']),
                'interest_rate': convert_to_decimal_and_quantize(new_data['interest_rate']),
                'estimated_delivery_price': convert_to_decimal_and_quantize(new_data['estimated_delivery_price']),
                'mark_iv': convert_to_decimal_and_quantize(new_data['mark_iv']),
                'bid_iv': convert_to_decimal_and_quantize(new_data['bid_iv']),
                'ask_iv': convert_to_decimal_and_quantize(new_data['ask_iv']),
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
            ts = trade['timestamp']
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
                    'price': convert_to_decimal_and_quantize(trade['price']),
                    'iv': convert_to_decimal_and_quantize(trade['iv']),
                    'direction': trade['direction'],
                    'index_price': convert_to_decimal_and_quantize(trade['index_price']),
                    'instrument_name': trade['instrument_name'],
                    'trade_seq': trade['trade_seq'],
                    'amount': convert_to_decimal_and_quantize(trade['amount']),
                    'mark_price': convert_to_decimal_and_quantize(trade['mark_price']),
                    'tick_direction': int(trade['tick_direction']),
                    'starbase_match_id': convert_to_decimal_and_quantize(trade['starbase_match_id']),
                    'trade_id': convert_to_decimal_and_quantize(trade['trade_id']),
                    'contracts': convert_to_decimal_and_quantize(trade['contracts']),
                    'starbase_timestamp': convert_to_decimal_and_quantize(trade['starbase_timestamp']),
                }
            except (KeyError, IndexError, TypeError):
                continue

            trade_list.append(trade)

        return trade_list
