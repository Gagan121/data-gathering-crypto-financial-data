import time
from datetime import datetime
from decimal import Decimal
from core.exchanges.exchange_adapter import ExchangeAdapter,flatten, convert_to_decimal_and_quantize
import os
from dotenv import load_dotenv
load_dotenv()


class DeribitPerpetualAdapter(ExchangeAdapter):
    def __init__(self, channels:list, url:str, msg:dict, exchange_name:str, ticker:str, heart_beat_msg:dict, heart_beat_reply_msg:dict) -> None:
        super().__init__(channels=channels, exchange_name=exchange_name, url=url, msg=msg, ticker=ticker, heart_beat_msg=heart_beat_msg, heart_beat_reply_msg=heart_beat_reply_msg)
        load_dotenv()
        self._client_id = os.getenv("DERIBIT_PERPETUAL_CLIENT_ID")
        self._client_secret = os.getenv("DERIBIT_PERPETUAL_CLIENT_SECRET")


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
        valid:bool = ((isinstance(authentication_message, dict)
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
                        and isinstance(msg["params"]['data'],list)
                )

        return outcome



    def restructure_data(self, data) -> dict|list:

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
        temp = (ts/1000)
        exch_ts_sec = int(temp)
        exch_ts_micro = int((temp - exch_ts_sec) * 1_000_000)

        sys_ts_sec = int(sys_time)
        sys_ts_micro = int((sys_time - sys_ts_sec) * 1_000_000)

        new_data = flatten(data["params"]["data"])
        # removing the keys from the dictionary
        new_data.pop('state', None)
        new_data.pop('instrument_name', None)

        try:
            return {"channel" : data['params']['channel'],
            'exch_ts_sec': exch_ts_sec,
            'exch_ts_micro': exch_ts_micro,
            'sys_ts_sec': sys_ts_sec,
            'sys_ts_micro': sys_ts_micro,
            'bid' : convert_to_decimal_and_quantize(new_data['best_bid_price']),
            'ask' : convert_to_decimal_and_quantize(new_data['best_ask_price']),
            'bid_quantity' : convert_to_decimal_and_quantize(new_data['best_bid_amount']),
            'ask_quantity' : convert_to_decimal_and_quantize(new_data['best_ask_amount']),
            'high' : convert_to_decimal_and_quantize(new_data['high']),
            'low' : convert_to_decimal_and_quantize(new_data['low']),
            'price_change' : convert_to_decimal_and_quantize(new_data['price_change']),
            'volume' : convert_to_decimal_and_quantize(new_data['volume']),
            'volume_usd' : convert_to_decimal_and_quantize(new_data['volume_usd']),
            'volume_notional' : convert_to_decimal_and_quantize(new_data['volume_notional']),
            'index_price' : convert_to_decimal_and_quantize(new_data['index_price']),
            'last_price' : convert_to_decimal_and_quantize(new_data['last_price']),
            'settlement_price' : convert_to_decimal_and_quantize(new_data['settlement_price']),
            'min_price' : convert_to_decimal_and_quantize(new_data['min_price']),
            'max_price' : convert_to_decimal_and_quantize(new_data['max_price']),
            'open_interest' : convert_to_decimal_and_quantize(new_data['open_interest']),
            'mark_price' : convert_to_decimal_and_quantize(new_data['mark_price']),
            'interest_value' : convert_to_decimal_and_quantize(new_data['interest_value']),
            'current_funding' : convert_to_decimal_and_quantize(new_data['current_funding']),
            'estimated_delivery_price' : convert_to_decimal_and_quantize(new_data['estimated_delivery_price']),
            'funding_8h' : convert_to_decimal_and_quantize(new_data['funding_8h']),
            }
        #     need to add more points of data, funding rate and other stuff
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
            temp = (ts/1000)
            exch_ts_sec = int(temp)
            exch_ts_micro = int((temp - exch_ts_sec) * 1_000_000)

            try:
                trade = {
                    "channel": data['params']['channel'],
                    'sys_ts_sec' : sys_ts_sec,
                    'sys_ts_micro' : sys_ts_micro,
                    'exch_ts_sec' : exch_ts_sec,
                    'exch_ts_micro' : exch_ts_micro,
                    'price' : convert_to_decimal_and_quantize(trade['price']),
                    'direction' : trade['direction'],
                    'index_price' : convert_to_decimal_and_quantize(trade['index_price']),
                    'instrument_name' : trade['instrument_name'],
                    'trade_seq' : trade['trade_seq'],
                    'amount' : convert_to_decimal_and_quantize(trade['amount']),
                    'mark_price' : convert_to_decimal_and_quantize(trade['mark_price']),
                    'tick_direction' : int(trade['tick_direction']),
                    'starbase_match_id' : convert_to_decimal_and_quantize(trade['starbase_match_id']),
                    'trade_id' : convert_to_decimal_and_quantize(trade['trade_id']),
                    'contracts' : convert_to_decimal_and_quantize(trade['contracts']),
                    'starbase_timestamp' : convert_to_decimal_and_quantize(trade['starbase_timestamp']),
                }
            except (KeyError, IndexError, TypeError):
                continue

            trade_list.append(trade)


        return trade_list

