from typing import override

from core.exchanges.exchange_adapter import ExchangeAdapter
from datetime import datetime
from decimal import Decimal

class BinanceAdapter(ExchangeAdapter):
    def get_authentication_info(self) -> dict | None:
        pass

    def get_refresh_authentication_info(self) -> dict:
        pass

    def validate_authentication(self, authentication_message) -> bool:
        pass

    def __init__(self, channels:list, websocket_url:str, msg:dict, exchange_name:str, ticker:str):
        super().__init__(channels=channels, exchange_name=exchange_name, websocket_url=websocket_url, msg=msg, ticker=ticker)

    @override
    def valid_message_can_pass_and_restructure_data(self, msg) -> dict:
        bool_1 = self.validate_message(msg)
        if bool_1:
            # if data doesn't have the right keys
            data = self.restructure_data(msg)
            # encase our data is empty then
            bool_2 = bool(data)
            # data is saved on the exchange adapter for the time being as its a large piece of data, and remaking is a waste of time
            # dict means tickers, list means trades

            bool_3 = False
            # if we ignore the duplication and just see what comes in for now we can see if any patterns come up
            if isinstance(data, dict):
                bool_3 = self.check_quotes_diff(data)

            return {
                "valid" : (bool_1 and bool_2 and bool_3),
                "data" : data
            }

        return {"valid" : False, "data" : dict()}

    def validate_message(self, msg):
        return (
            isinstance(msg, dict)
            and (not "error" in msg)
            and "E" in msg
            and "a" in msg
            and "A" in msg
            and "b" in msg
            and "B" in msg
        )
    # if we plan on adding trade data this will have to be sorted like the deribit adapter and channel would need to change, channel[0],
    def restructure_data(self, data) -> dict:

        if 'exch_ts_sec' in data:
            return data

        ts = data["E"]
        sys_time = data['sys_time']
        normalised_ts = ts / 1000
        exch_ts_sec = int(normalised_ts)
        exch_ts_micro = int((normalised_ts - exch_ts_sec) * 1_000_000)

        sys_ts_sec = int(sys_time)
        sys_ts_micro = int((sys_time - sys_ts_sec) * 1_000_000)

        try:
            bid = Decimal(data['b'])
            ask = Decimal(data['a'])
            price = (ask + bid) / Decimal(2)
            bid_quantity = Decimal(data['B'])
            ask_quantity = Decimal(data['A'])
        except (KeyError, IndexError, TypeError):
            return dict()

        return {
            "channel": self.channels[0],
            'exch_ts_sec': exch_ts_sec,
            'exch_ts_micro': exch_ts_micro,
            'sys_ts_sec': sys_ts_sec,
            'sys_ts_micro': sys_ts_micro,
            'price': price,
            'bid': bid,
            'ask': ask,
            'bid_quantity': bid_quantity,
            'ask_quantity': ask_quantity,
        }