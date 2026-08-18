from core.exchanges.exchange_adapter import ExchangeAdapter
from datetime import datetime
from decimal import Decimal

class BinanceAdapter(ExchangeAdapter):
    def __init__(self, url:str, msg:dict, exchange_name:str, ticker:str) -> None:
        super().__init__(exchange_name=exchange_name, url=url, msg=msg, ticker=ticker)


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

    def get_structure_of_data(self, data) -> dict:
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
            price = (ask + bid) / 2
            bid_quantity = Decimal(data['B'])
            ask_quantity = Decimal(data['A'])
        except (KeyError, IndexError, TypeError):
            return dict()

        return {
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