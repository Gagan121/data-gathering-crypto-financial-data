import pytest
from core.exchanges.coinbase_adapter import CoinbaseAdapter
from decimal import Decimal

@pytest.fixture
def adapter():
    return CoinbaseAdapter(
        exchange_name="Coinbase",
        ticker='BTC_USD',
        url="wss://advanced-trade-ws.coinbase.com",
        msg={
            "type": "subscribe",
            "product_ids": ["BTC-USD"],
            "channel": "ticker"
        }
    )

@pytest.fixture
def model_data():
    return {
        "sys_time": 1734282093,
        'channel': 'ticker',
        'timestamp': '2026-06-06T17:05:43.761792129Z',
        'sequence_num': 0,
        'events': [
            {
                'type': 'snapshot',
                'tickers': [
                    {
                        'type': 'ticker',
                        'product_id': 'BTC-USD',
                        'price': '60696.8',
                        'volume_24_h': '17543.63809816',
                        'low_24_h': '59073.01',
                        'high_24_h': '61971.74',
                        'low_52_w': '59073.01',
                        'high_52_w': '126296',
                        'price_percent_chg_24_h': '-0.91560764299268',
                        'best_bid': '60695.94',
                        'best_ask': '60695.95',
                        'best_bid_quantity': '0.03004871',
                        'best_ask_quantity': '0.02210567'
                    }
                ]
            }
        ]
    }


def test_validate_message_valid_message(adapter, model_data):

    assert adapter.valid_message_can_pass_and_restructure_data(model_data) == True

def test_validate_message_duplicate_prices(adapter, model_data):
    assert adapter.valid_message_can_pass_and_restructure_data(model_data) == True
    assert adapter.valid_message_can_pass_and_restructure_data(model_data) == False
    model_data['events'][0]['tickers'][0]['best_bid'] = Decimal('10000')
    assert adapter.valid_message_can_pass_and_restructure_data(model_data) == True

def test_validate_message_invalid_message(adapter):
    msg = {
        "sys_time": 1734282093,
        'channel': 'ticker',
        'timestamp': '2026-06-06T17:05:43.761792129Z',
        'sequence_num': 0,
        'events': [
            {
                'type': 'snapshot',
                'tickers': [
                    {
                    }
                ]
            }
        ]
    }

    assert adapter.valid_message_can_pass_and_restructure_data(msg) == False


def test_normalise_data_correct_data(adapter, model_data):
    batch = [model_data, model_data]

    normalise_list_of_data = adapter.normalise_data(batch)

    model_list_of_normalised_data = [
        {
            'ask': Decimal('60695.95'),
            'ask_quantity': Decimal('0.02210567'),
            'bid': Decimal('60695.94'),
            'bid_quantity': Decimal('0.03004871'),
            'exch_ts_micro': 761792,
            'exch_ts_sec': 1780765543,
            'price': Decimal('60696.8'),
            'sys_ts_micro': 0,
            'sys_ts_sec': 1734282093
        },
        {
            'ask': Decimal('60695.95'),
            'ask_quantity': Decimal('0.02210567'),
            'bid': Decimal('60695.94'),
            'bid_quantity': Decimal('0.03004871'),
            'exch_ts_micro': 761792,
            'exch_ts_sec': 1780765543,
            'price': Decimal('60696.8'),
            'sys_ts_micro': 0,
            'sys_ts_sec': 1734282093
        }
    ]
    assert normalise_list_of_data == model_list_of_normalised_data


def test_normalise_data_missing_data(adapter, model_data):
    missing_data = {
        "sys_time": 1734282093,
        'channel': 'ticker',
        'timestamp': '2026-06-06T17:05:43.761792129Z',
        'sequence_num': 0,
        'events': [
            {
                'type': 'snapshot',
                'tickers': [
                    {
                    }
                ]
            }
        ]
    }

    batch = [model_data, missing_data]

    normalised_list_of_data = adapter.normalise_data(batch)

    model_list_of_normalised_data = [
        {
            'ask': Decimal('60695.95'),
            'ask_quantity': Decimal('0.02210567'),
            'bid': Decimal('60695.94'),
            'bid_quantity': Decimal('0.03004871'),
            'exch_ts_micro': 761792,
            'exch_ts_sec': 1780765543,
            'price': Decimal('60696.8'),
            'sys_ts_micro': 0,
            'sys_ts_sec': 1734282093
        }
    ]

    assert normalised_list_of_data == model_list_of_normalised_data

