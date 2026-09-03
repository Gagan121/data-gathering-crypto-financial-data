import pytest
from core.exchanges.binance_adapter import BinanceAdapter
from decimal import Decimal

@pytest.fixture
def adapter():
    # channels: list, url: str, msg: dict, exchange_name: str, ticker: str
    return BinanceAdapter(
        channels=["ticker"],
        websocket_url="wss://fstream.binance.com/public/ws/btcusdt@bookTicker",
        msg={
            "method": "SUBSCRIBE",
            "params":
                [
                    "btcusdt@bookTicker"
                ],
            "id": 1
        },
        exchange_name="Binance",
        ticker="BTC_USDT"
    )

@pytest.fixture
def model_data():
    return {
        'A': '3.159',
        'B': '6.071',
        'E': 1781201974537,
        'T': 1781201974537,
        'a': '63518.40',
        'b': '63518.30',
        'e': 'bookTicker',
        's': 'BTCUSDT',
        'sys_time': 1781201974.7109642,
        'u': 10784235188342
    }


def test_validate_message_valid_message(adapter, model_data):
    outcome = adapter.valid_message_can_pass_and_restructure_data(model_data)
    assert outcome["valid"] == True

# def test_validate_message_duplicate_prices(adapter, model_data):
#     assert adapter.valid_message_can_pass_and_restructure_data(model_data) == True
#     assert adapter.valid_message_can_pass_and_restructure_data(model_data) == False
#     model_data['a'] = '60000'
#     assert adapter.valid_message_can_pass_and_restructure_data(model_data) == True


def test_validate_message_invalid_message(adapter):
    msg = {
        'B': '6.071',
        'E': 1781201974537,
        'T': 1781201974537,
        'e': 'bookTicker',
        's': 'BTCUSDT',
        'sys_time': 1781201974.7109642,
        'u': 10784235188342
    }

    outcome = adapter.valid_message_can_pass_and_restructure_data(msg)

    assert outcome["valid"] == False


def test_normalise_data_correct_data(adapter, model_data):
    batch = [model_data, model_data]
    normalise_list_of_data = adapter.normalise_data(batch)
    model_list_of_normalised_data = [
        {
            "channel":"ticker",
            'ask': Decimal('63518.40'),
            'ask_quantity': Decimal('3.159'),
            'bid': Decimal('63518.30'),
            'bid_quantity': Decimal('6.071'),
            'exch_ts_micro': 536999,
            'exch_ts_sec': 1781201974,
            'price': Decimal('63518.35'),
            'sys_ts_micro': 710964,
            'sys_ts_sec': 1781201974
        },
        {
            "channel": "ticker",
            'ask': Decimal('63518.40'),
            'ask_quantity': Decimal('3.159'),
            'bid': Decimal('63518.30'),
            'bid_quantity': Decimal('6.071'),
            'exch_ts_micro': 536999,
            'exch_ts_sec': 1781201974,
            'price': Decimal('63518.35'),
            'sys_ts_micro': 710964,
            'sys_ts_sec': 1781201974
        },
    ]
    assert normalise_list_of_data == model_list_of_normalised_data


def test_normalise_data_missing_data(adapter, model_data):
    missing_data = {
        'B': '6.071',
        'E': 1781201974537,
        'T': 1781201974537,
        'e': 'bookTicker',
        's': 'BTCUSDT',
        'sys_time': 1781201974.7109642,
        'u': 10784235188342
    }

    batch = [model_data, missing_data]

    normalised_list_of_data = adapter.normalise_data(batch)

    model_list_of_normalised_data = [
        {
            "channel": "ticker",
            'ask': Decimal('63518.40'),
            'ask_quantity': Decimal('3.159'),
            'bid': Decimal('63518.30'),
            'bid_quantity': Decimal('6.071'),
            'exch_ts_micro': 536999,
            'exch_ts_sec': 1781201974,
            'price': Decimal('63518.35'),
            'sys_ts_micro': 710964,
            'sys_ts_sec': 1781201974
        }
    ]

    assert normalised_list_of_data == model_list_of_normalised_data