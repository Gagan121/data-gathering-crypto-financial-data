from core.exchanges.exchange_adapter import ExchangeAdapter
from datetime import datetime

class CoinbaseAdapter(ExchangeAdapter):
    def __init__(self, path_to_folder:str, url:str, msg:dict) -> None:
        super().__init__(path_to_folder, exchange_name="Coinbase", url=url, msg=msg)

    def validate_message(self, msg):
        return (
            isinstance(msg, dict)
            and "events" in msg
            and len(msg["events"]) > 0
        )


    def normalise_data(self, batch_list:list) -> list:
        normalised_data = []
        for data in batch_list:
            ts = data["timestamp"]
            sys_time = data['sys_time']
            new_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            exch_ts_sec = int(new_dt.timestamp())
            exch_ts_micro = new_dt.microsecond

            sys_ts_sec = int(sys_time)
            sys_ts_micro = int((sys_time - sys_ts_sec) * 1000)

            try:
                price = data['events'][0]['tickers'][0]['price']
                bid = data['events'][0]['tickers'][0]['best_bid']
                ask = data['events'][0]['tickers'][0]['best_ask']
                bid_quantity = data['events'][0]['tickers'][0]['best_bid_quantity']
                ask_quantity = data['events'][0]['tickers'][0]['best_ask_quantity']
            except (KeyError, IndexError, TypeError):
                continue

            norm_data = {
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

            normalised_data.append(norm_data)

        return normalised_data