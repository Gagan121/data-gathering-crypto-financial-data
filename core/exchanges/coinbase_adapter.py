from core.exchanges.exchange_adapter import ExchangeAdapter
from datetime import datetime
from decimal import Decimal

class CoinbaseAdapter(ExchangeAdapter):
    def get_authentication_info(self) -> dict | None:
        pass

    def get_refresh_authentication_info(self) -> dict:
        pass

    def validate_authentication(self, authentication_message) -> bool:
        pass

    def __init__(self,channels:list, url:str, msg:dict, exchange_name:str, ticker:str) -> None:
        super().__init__(channels=channels, exchange_name=exchange_name, url=url, msg=msg, ticker=ticker)

    def validate_message(self, msg) -> bool:
        return (
            isinstance(msg, dict)
            and "timestamp" in msg
            and "events" in msg
            and len(msg["events"]) > 0
            and "tickers" in msg["events"][0]
            and len(msg["events"][0]["tickers"]) > 0
            and "price" in msg["events"][0]["tickers"][0]
            and "best_bid" in msg["events"][0]["tickers"][0]
            and "best_ask" in msg["events"][0]["tickers"][0]
            and "best_bid_quantity" in msg["events"][0]["tickers"][0]
            and "best_ask_quantity" in msg["events"][0]["tickers"][0]
        )

    def restructure_data(self, data) -> dict:

        if 'exch_ts_sec' in data:
            return data

        ts = data["timestamp"]
        sys_time = data['sys_time']
        new_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        exch_ts_sec = int(new_dt.timestamp())
        exch_ts_micro = new_dt.microsecond

        sys_ts_sec = int(sys_time)
        sys_ts_micro = int((sys_time - sys_ts_sec) * 1_000_000)

        try:
            price = Decimal(data['events'][0]['tickers'][0]['price'])
            bid = Decimal(data['events'][0]['tickers'][0]['best_bid'])
            ask = Decimal(data['events'][0]['tickers'][0]['best_ask'])
            bid_quantity = Decimal(data['events'][0]['tickers'][0]['best_bid_quantity'])
            ask_quantity = Decimal(data['events'][0]['tickers'][0]['best_ask_quantity'])
            channel = data["channel"]
        except (KeyError, IndexError, TypeError):
            return dict()

        return {
            "channel": channel,
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
