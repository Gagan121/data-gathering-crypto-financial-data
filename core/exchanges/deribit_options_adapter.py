from exchange_adapter import ExchangeAdapter



class DeribitOptionsAdapter(ExchangeAdapter):
    def __init__(self, url:str, msg:dict, exchange_name:str, ticker:str, heart_beat_msg:dict, heart_beat_reply_msg:dict) -> None:
        super().__init__(exchange_name=exchange_name, url=url, msg=msg, ticker=ticker, heart_beat_msg=heart_beat_msg, heart_beat_reply_msg=heart_beat_reply_msg)
        load_dotenv()
        self._client_id = os.getenv("DERIBIT_CLIENT_ID")
        self._client_secret = os.getenv("DERIBIT_CLIENT_SECRET")


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
        return (
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


    def restructure_data(self, data) -> dict:

        if 'exch_ts_sec' in data:
            return data

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
            bid = Decimal(new_data['best_bid_price'])
            ask = Decimal(new_data['best_ask_price'])
            bid_quantity = Decimal(new_data['best_bid_amount'])
            ask_quantity = Decimal(new_data['best_ask_amount'])
            high = Decimal(new_data['high'])
            low = Decimal(new_data['low'])
            price_change = Decimal(new_data['price_change'])
            volume = Decimal(new_data['volume'])
            volume_usd = Decimal(new_data['volume_usd'])
            volume_notional = Decimal(new_data['volume_notional'])
            index_price = Decimal(new_data['index_price'])
            last_price = Decimal(new_data['last_price'])
            settlement_price = Decimal(new_data['settlement_price'])
            min_price = Decimal(new_data['min_price'])
            max_price = Decimal(new_data['max_price'])
            open_interest = Decimal(new_data['open_interest'])
            mark_price = Decimal(new_data['mark_price'])
            interest_value = Decimal(new_data['interest_value'])
            current_funding = Decimal(new_data['current_funding'])
            estimated_delivery_price = Decimal(new_data['estimated_delivery_price'])
            funding_8h = Decimal(new_data['funding_8h'])
        #     need to add more points of data, funding rate and other stuff
        except (KeyError, IndexError, TypeError):
            return dict()
        # the values are restricted to the number of decimal points used
        return  {
            'exch_ts_sec': exch_ts_sec,
            'exch_ts_micro': exch_ts_micro,
            'sys_ts_sec': sys_ts_sec,
            'sys_ts_micro': sys_ts_micro,
            'bid': bid.quantize(Decimal("0.00000001")),
            'ask': ask.quantize(Decimal("0.00000001")),
            'bid_quantity': bid_quantity.quantize(Decimal("0.000000001")),
            'ask_quantity': ask_quantity.quantize(Decimal("0.000000001")),
            'high' : high.quantize(Decimal("0.00000001")),
            'low' : low.quantize(Decimal("0.00000001")),
            'price_change' : price_change.quantize(Decimal("0.000000000001")),
            'volume' : volume.quantize(Decimal("0.00000000001")),
            'volume_usd' : volume_usd.quantize(Decimal("0.0000000001")),
            'volume_notional' : volume_notional.quantize(Decimal("0.0000000001")),
            'index_price' : index_price.quantize(Decimal("0.00000001")),
            'last_price' : last_price.quantize(Decimal("0.00000001")),
            'settlement_price' : settlement_price.quantize(Decimal("0.00000001")),
            'min_price' : min_price.quantize(Decimal("0.00000001")),
            'max_price' : max_price.quantize(Decimal("0.00000001")),
            'open_interest' : open_interest.quantize(Decimal("0.0000000000000001")),
            'mark_price' : mark_price.quantize(Decimal("0.00000001")),
            'interest_value' : interest_value.quantize(Decimal("0.00000000000000000001")),
            'current_funding' : current_funding.quantize(Decimal("0.000000000000000000001")),
            'estimated_delivery_price' : estimated_delivery_price.quantize(Decimal("0.000000000001")),
            'funding_8h' : funding_8h.quantize(Decimal("0.0000000000000001")),
        }
