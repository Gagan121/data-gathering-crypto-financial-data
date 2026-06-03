from core.exchanges.exchange_adapter import ExchangeAdapter
from datetime import datetime

class CoinbaseAdapter(ExchangeAdapter):
    def __init__(self, path_to_folder:str, url:str, msg:dict) -> None:
        super().__init__(path_to_folder, exchange_name="Coinbase", url=url, msg=msg)

    def validate_message(self, msg):
        return "events" in msg and msg["events"]


    def normalise_data(self):
        normalised_data = []
        for data in self.batch_list:
            ts = data["timestamp"]
            sys_time = data['sys_time']
            new_timestamp_numerical = datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
            exch_ts_sec, exch_ts_micro = [int(item) for item in str(new_timestamp_numerical).split('.')]
            sys_ts_sec, sys_ts_micro = [int(item) for item in str(round(sys_time, 6)).split('.')]

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

        self.normalised_list_of_data =  normalised_data