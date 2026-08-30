import time

import pytest
from core.exchanges.deribit_options_adapter import DeribitOptionsAdapter
from decimal import Decimal
import json
from core.websockets.websocket_client import WebsocketClient


@pytest.fixture
def adapter_ticker():
    return DeribitOptionsAdapter(
        channels = ["ticker.BTC-25DEC26-140000-C.agg2",],
        exchange_name="Deribit",
        ticker="BTC",
        websocket_url="wss://www.deribit.com/ws/api/v2",
        # url="wss://test.deribit.com/ws/api/v2",
        msg={
            "jsonrpc": "2.0",
            "method": "public/subscribe",
            "id": 42,
            "params": {
                "channels": [
                    "ticker.BTC-25DEC26-140000-C.agg2",

                ]
            }
        },
        heart_beat_msg={
            "jsonrpc": "2.0",
            "id": 10,
            "method": "public/set_heartbeat",
            "params": {"interval": 30}
        },
        heart_beat_reply_msg={
            "jsonrpc": "2.0",
            "method": "public/test",
            "params": {},
            "id": 1
        }
    )


@pytest.fixture
def adapter_trade():
    return DeribitOptionsAdapter(
        channels = ["trades.BTC-25DEC26-140000-C.agg2",],
        exchange_name="Deribit",
        ticker="BTC",
        websocket_url="wss://www.deribit.com/ws/api/v2",
        # url="wss://test.deribit.com/ws/api/v2",
        msg={
            "jsonrpc": "2.0",
            "method": "public/subscribe",
            "id": 42,
            "params": {
                "channels": [
                    "trades.BTC-25DEC26-140000-C.agg2"
                ]
            }
        },
        heart_beat_msg={
            "jsonrpc": "2.0",
            "id": 10,
            "method": "public/set_heartbeat",
            "params": {"interval": 30}
        },
        heart_beat_reply_msg={
            "jsonrpc": "2.0",
            "method": "public/test",
            "params": {},
            "id": 1
        }
    )


@pytest.fixture
def model_data_ticker():
    return {
        'jsonrpc': '2.0',
        'method': 'subscription',
        'params': {
            'channel': 'ticker.BTC-25JUN27-98000-P.agg2',
            'data': {
                'timestamp': 1788124051207,
                'state': 'open',
                'stats': {
                    'high': None,
                    'low': None,
                    'price_change': None,
                    'volume': 0.0,
                    'volume_usd': 0.0
                },
                'greeks': {
                    'delta': -0.61575,
                    'gamma': 1e-05,
                    'vega': 281.91318,
                    'theta': -19.77072,
                    'rho': -600.5794
                },
                'index_price': 78442.28,
                'instrument_name': 'BTC-25JUN27-98000-P',
                'last_price': None,
                'settlement_price': 0.28902854,
                'min_price': 0.2275,
                'max_price': 0.3605,
                'open_interest': 0.0,
                'mark_price': 0.2843,
                'interest_rate': 0.0,
                'estimated_delivery_price': 78442.28,
                'best_ask_price': 0.287,
                'best_bid_price': 0.282, 'mark_iv': 41.86,
                'bid_iv': 41.2, 'ask_iv': 42.64,
                'underlying_price': 81606.61,
                'underlying_index': 'BTC-25JUN27',
                'best_ask_amount': 2.6,
                'best_bid_amount': 1.0
            }
        },
        'sys_time': 1788124054.024729
    }



@pytest.fixture
def model_data_trades():
    return {
        'jsonrpc': '2.0',
        'method': 'subscription',
        'params': {
            'channel': 'trades.BTC-31AUG26-78000-C.agg2',
            'data': [
                {
                    'amount': 0.2,
                    'contracts': 0.2,
                    'direction': 'sell',
                    'index_price': 78436.64,
                    'instrument_name': 'BTC-31AUG26-78000-C',
                    'iv': 29.28,
                    'mark_price': 0.00813229,
                    'price': 0.0075,
                    'starbase_match_id': 219926831833296896,
                    'starbase_timestamp': 1788124242752134005,
                    'tick_direction': 2,
                    'timestamp': 1788124242752,
                    'trade_id': '442891539',
                    'trade_seq': 42
                }
            ]
        },
        'sys_time': 1788124245.012731
    }

# this could take some time to run -> the timer between heartbeat is 30 seconds
@pytest.mark.asyncio
async def test_heartbeat(adapter_ticker):
    wc = WebsocketClient(exchange_adapter=adapter_ticker)

    await wc.connect()
    # creates the authentication for a year
    await wc.set_heart_beat()
    start = time.time()

    counter = 0
    try:
        async for message in wc.ws:
            data = json.loads(message)

            await wc.filter_message_and_respond(data)

            if ("method" in data) and (data["method"] == "heartbeat"):
                time_take = time.time() - start
                start = time.time()
                # print(time_take)
                counter += 1

            if counter >= 3:
                break
    finally:
        await wc.ws.close()

    assert counter >= 3


@pytest.mark.asyncio
async def test_authenticate(adapter_ticker):
    wc = WebsocketClient(exchange_adapter=adapter_ticker)

    await wc.connect()
    # creates the authentication for a year
    await wc.authenticate()

    assert adapter_ticker.if_refresh_token_exists() == True

def test_valid_message_ticker(adapter_ticker, model_data_ticker):
    outcome = adapter_ticker.valid_message_can_pass_and_restructure_data(model_data_ticker)
    assert outcome['valid'] == True

def test_invalid_message_ticker(adapter_ticker):
    msg = {
        'jsonrpc': '2.0',
        'method': 'subscription',
        'params': {
            'channel': 'ticker.BTC-25JUN27-98000-P.agg2',
            'data': {
                'timestamp': 1788124051207,
                'state': 'open'
            },
        },
        'sys_time': 1788124054.024729
    }
    outcome = adapter_ticker.valid_message_can_pass_and_restructure_data(msg)

    assert outcome["valid"] == False

def test_validate_message_valid_message_trade(adapter_trade, model_data_trades):
    outcome = adapter_trade.valid_message_can_pass_and_restructure_data(model_data_trades)
    assert outcome['valid'] == True


def test_invalid_message_trade(adapter_trade):
    msg = {
        'jsonrpc': '2.0',
        'method': 'subscription',
        'params': {
            'channel': 'ticker.BTC-25JUN27-98000-P.agg2',
            'data': []
        },
        'sys_time': 1787738702.5461586
    }
    outcome = adapter_trade.valid_message_can_pass_and_restructure_data(msg)

    assert outcome["valid"] == False

def test_validate_message_invalid_message_trade(adapter_trade):
    msg = {
        'jsonrpc': '2.0',
        'method': 'subscription',
        'params': {
            'channel': 'trades.BTC-31AUG26-78000-C.agg2',
            'data': [
                {
                    'amount': 0.2,
                    'contracts': 0.2,
                    'direction': 'sell',
                    'index_price': 78436.64,
                    'instrument_name': 'BTC-31AUG26-78000-C',
                    'iv': 29.28,
                    'mark_price': 0.00813229,
                    'price': 0.0075,
                    'starbase_match_id': 219926831833296896,
                    'starbase_timestamp': 1788124242752134005,
                    'tick_direction': 2,
                    'timestamp': 1788124242752,
                    'trade_id': '442891539',
                    'trade_seq': 42
                }
            ]
        },
        'sys_time': 1788124245.012731
    }
    # normalise
    outcome = adapter_trade.valid_message_can_pass_and_restructure_data(msg)

    model_list_of_restructured_data = [
        {
            'channel': 'trades.BTC-31AUG26-78000-C.agg2',
            'sys_ts_sec': 1788124245,
            'sys_ts_micro': 12731,
            'exch_ts_sec': 1788124242,
            'exch_ts_micro': 752000,
            'price': Decimal('0.0075000'),
            'iv': Decimal('29.2800000'),
            'direction': 'sell',
            'index_price': Decimal('78436.6400000'),
            'instrument_name': 'BTC-31AUG26-78000-C',
            'trade_seq': 42,
            'amount': Decimal('0.2000000'),
            'mark_price': Decimal('0.0081323'),
            'tick_direction': 2,
            'starbase_match_id': 219926831833296896,
            'trade_id': Decimal('442891539'),
            'contracts': Decimal('0.2000000'),
            'starbase_timestamp': 1788124242752134005
        }
    ]


    assert outcome["data"] == model_list_of_restructured_data


def test_normalise_data_correct_data_ticker(adapter_ticker, model_data_ticker):
    batch = [model_data_ticker, model_data_ticker]

    normalise_list_of_data = adapter_ticker.normalise_data(batch)

    model_list_of_normalised_data = [
        {
            'channel': 'ticker.BTC-25JUN27-98000-P.agg2',
            'exch_ts_sec': 1788124051,
            'exch_ts_micro': 207000,
            'sys_ts_sec': 1788124054,
            'sys_ts_micro': 24729,
            'underlying_index': 'BTC-25JUN27',
            'underlying_price': Decimal('81606.6100000'),
            'bid': Decimal('0.2820000'),
            'ask': Decimal('0.2870000'),
            'bid_quantity': Decimal('1.0000000'),
            'ask_quantity': Decimal('2.6000000'),
            'high': Decimal('0'),
            'low': Decimal('0'),
            'price_change': Decimal('0'),
            'volume': Decimal('0E-7'),
            'volume_usd': Decimal('0E-7'),
            'delta': Decimal('-0.6157500'),
            'gamma': Decimal('0.0000100'),
            'vega': Decimal('281.9131800'),
            'theta': Decimal('-19.7707200'),
            'rho': Decimal('-600.5794000'),
            'index_price': Decimal('78442.2800000'),
            'last_price': Decimal('0'),
            'settlement_price': Decimal('0.2890285'),
            'min_price': Decimal('0.2275000'),
            'max_price': Decimal('0.3605000'),
            'open_interest': Decimal('0E-7'),
            'mark_price': Decimal('0.2843000'),
            'interest_rate': Decimal('0E-7'),
            'estimated_delivery_price': Decimal('78442.2800000'),
            'mark_iv': Decimal('41.8600000'),
            'bid_iv': Decimal('41.2000000'),
            'ask_iv': Decimal('42.6400000')
        },{
            'channel': 'ticker.BTC-25JUN27-98000-P.agg2',
            'exch_ts_sec': 1788124051,
            'exch_ts_micro': 207000,
            'sys_ts_sec': 1788124054,
            'sys_ts_micro': 24729,
            'underlying_index': 'BTC-25JUN27',
            'underlying_price': Decimal('81606.6100000'),
            'bid': Decimal('0.2820000'),
            'ask': Decimal('0.2870000'),
            'bid_quantity': Decimal('1.0000000'),
            'ask_quantity': Decimal('2.6000000'),
            'high': Decimal('0'),
            'low': Decimal('0'),
            'price_change': Decimal('0'),
            'volume': Decimal('0E-7'),
            'volume_usd': Decimal('0E-7'),
            'delta': Decimal('-0.6157500'),
            'gamma': Decimal('0.0000100'),
            'vega': Decimal('281.9131800'),
            'theta': Decimal('-19.7707200'),
            'rho': Decimal('-600.5794000'),
            'index_price': Decimal('78442.2800000'),
            'last_price': Decimal('0'),
            'settlement_price': Decimal('0.2890285'),
            'min_price': Decimal('0.2275000'),
            'max_price': Decimal('0.3605000'),
            'open_interest': Decimal('0E-7'),
            'mark_price': Decimal('0.2843000'),
            'interest_rate': Decimal('0E-7'),
            'estimated_delivery_price': Decimal('78442.2800000'),
            'mark_iv': Decimal('41.8600000'),
            'bid_iv': Decimal('41.2000000'),
            'ask_iv': Decimal('42.6400000')
        }
    ]


    assert normalise_list_of_data == model_list_of_normalised_data

# ------------------------------------------------------------------------
