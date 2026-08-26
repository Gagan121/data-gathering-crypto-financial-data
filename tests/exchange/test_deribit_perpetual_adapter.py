import pytest
from core.exchanges.deribit_perpetuals_adapter import DeribitPerpetualAdapter
from decimal import Decimal

@pytest.fixture
def adapter_ticker():
    return DeribitPerpetualAdapter(
        channels = ["ticker.BTC-PERPETUAL.raw",],
        exchange_name="Deribit",
        ticker="BTC_PERPETUAL",
        url="wss://www.deribit.com/ws/api/v2",
        # url="wss://test.deribit.com/ws/api/v2",
        msg={
            "jsonrpc": "2.0",
            "method": "public/subscribe",
            "id": 42,
            "params": {
                "channels": [
                    "ticker.BTC-PERPETUAL.raw"
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
    return DeribitPerpetualAdapter(
        channels = ["trades.BTC-PERPETUAL.raw"],
        exchange_name="Deribit",
        ticker="BTC_PERPETUAL",
        url="wss://www.deribit.com/ws/api/v2",
        # url="wss://test.deribit.com/ws/api/v2",
        msg={
            "jsonrpc": "2.0",
            "method": "public/subscribe",
            "id": 42,
            "params": {
                "channels": [
                    "trades.BTC-PERPETUAL.raw"
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
            'channel': 'ticker.BTC-PERPETUAL.raw',
            'data': {
                'timestamp': 1787738639997,
                'state': 'open',
                'stats': {
                    'high': 79561.5,
                    'low': 77859.0,
                    'price_change': -0.8092,
                    'volume': 5737.78723314,
                    'volume_usd': 452609800.0,
                    'volume_notional': 452609800.0},
                    'index_price': 78358.62,
                    'instrument_name': 'BTC-PERPETUAL',
                    'last_price': 78389.0,
                    'settlement_price': 78928.85,
                    'min_price': 77210.0,
                    'max_price': 79562.0,
                    'open_interest': 908588760,
                    'mark_price': 78385.9,
                    'interest_value': 0.009744204175182108,
                    'current_funding': 9.814e-05,
                    'estimated_delivery_price': 78358.62,
                    'funding_8h': 4.934e-05,
                    'best_ask_price': 78388.5,
                    'best_bid_price': 78388.0,
                    'best_ask_amount': 36580.0,
                    'best_bid_amount': 14410.0
            }
        },
        'sys_time': 1787738645.0862105
    }


@pytest.fixture
def model_data_trades():
    return {
        'jsonrpc': '2.0',
        'method': 'subscription',
        'params': {
            'channel': 'trades.BTC-PERPETUAL.raw',
            'data': [{
                'timestamp': 1787738641941,
                'price': 78394.0,
                'direction': 'buy',
                'index_price': 78361.43,
                'instrument_name': 'BTC-PERPETUAL',
                'trade_seq': 297373679,
                'amount': 140.0,
                'mark_price': 78388.92,
                'tick_direction': 0,
                'starbase_match_id': 218309504809316352,
                'trade_id': '442230029',
                'contracts': 14.0,
                'starbase_timestamp': 1787738641941535937
            },{
                'timestamp': 1787738641941,
                'price': 78394.0,
                'direction': 'buy',
                'index_price': 78361.43,
                'instrument_name': 'BTC-PERPETUAL',
                'trade_seq': 297373680,
                'amount': 420.0,
                'mark_price': 78388.92,
                'tick_direction': 1,
                'starbase_match_id': 218309504809316353,
                'trade_id': '442230030',
                'contracts': 42.0,
                'starbase_timestamp': 1787738641941535937
            }]
        },
        'sys_time': 1787738702.5461586
    }


def test_valid_message_ticker(adapter_ticker, model_data_ticker):
    outcome = adapter_ticker.valid_message_can_pass_and_restructure_data(model_data_ticker)
    assert outcome['valid'] == True

def test_invalid_message_ticker(adapter_ticker):
    msg = {
        'jsonrpc': '2.0',
        'method': 'subscription',
        'params': {
            'channel': 'ticker.BTC-PERPETUAL.raw',
            'data': {
                'timestamp': 1787738639997,
                'state': 'open'
            }
        },
        'sys_time': 1787738645.0862105
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
            'channel': 'trades.BTC-PERPETUAL.raw',
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
            'channel': 'trades.BTC-PERPETUAL.raw',
            'data': [{
                'timestamp': 1787738641941,
                'price': 78394.0,
                'direction': 'buy',
                'index_price': 78361.43,
                'instrument_name': 'BTC-PERPETUAL',
                'trade_seq': 297373679,
                'amount': 140.0,
                'mark_price': 78388.92,
                'tick_direction': 0,
                'starbase_match_id': 218309504809316352,
                'trade_id': '442230029',
                'contracts': 14.0,
                'starbase_timestamp': 1787738641941535937
            },{
                'timestamp': 1787738641941,
                'price': 78394.0
            }]
        },
        'sys_time': 1787738702.5461586
    }
    # normalise
    outcome = adapter_trade.valid_message_can_pass_and_restructure_data(msg)

    model_list_of_restructured_data = [{
        'channel': 'trades.BTC-PERPETUAL.raw',
        'sys_ts_sec': 1787738702,
        'sys_ts_micro': 546158,
        'exch_ts_sec': 1787738641,
        'exch_ts_micro': 940999,
        'price': Decimal('78394.00000000'),
        'direction': 'buy',
        'index_price': Decimal('78361.43000000'),
        'instrument_name': 'BTC-PERPETUAL',
        'trade_seq': 297373679,
        'amount': Decimal('140.00000000'),
        'mark_price': Decimal('78388.92000000'),
        'tick_direction': 0,
        'starbase_match_id': Decimal('218309504809316352.00000000'),
        'trade_id': Decimal('442230029.00000000'),
        'contracts': Decimal('14.00000000'),
        'starbase_timestamp': Decimal('1787738641941535937.00000000')
    }]

    assert outcome["data"] == model_list_of_restructured_data


def test_normalise_data_correct_data_ticker(adapter_ticker, model_data_ticker):
    batch = [model_data_ticker, model_data_ticker]

    normalise_list_of_data = adapter_ticker.normalise_data(batch)

    model_list_of_normalised_data = [{
        'channel': 'ticker.BTC-PERPETUAL.raw',
        'exch_ts_sec': 1787738639,
        'exch_ts_micro': 996999,
        'sys_ts_sec': 1787738645,
        'sys_ts_micro': 86210,
        'bid': Decimal('78388.00000000'),
        'ask': Decimal('78388.50000000'),
        'bid_quantity': Decimal('14410.000000000'),
        'ask_quantity': Decimal('36580.000000000'),
        'high': Decimal('79561.50000000'),
        'low': Decimal('77859.00000000'),
        'price_change': Decimal('-0.809200000000'),
        'volume': Decimal('5737.78723314000'),
        'volume_usd': Decimal('452609800.0000000000'),
        'volume_notional': Decimal('452609800.0000000000'),
        'index_price': Decimal('78358.62000000'),
        'last_price': Decimal('78389.00000000'),
        'settlement_price': Decimal('78928.85000000'),
        'min_price': Decimal('77210.00000000'),
        'max_price': Decimal('79562.00000000'),
        'open_interest': Decimal('908588760.0000000000000000'),
        'mark_price': Decimal('78385.90000000'),
        'interest_value': Decimal('0.00974420417518210777'),
        'current_funding': Decimal('0.000098140000000000006'),
        'estimated_delivery_price': Decimal('78358.619999999995'),
        'funding_8h': Decimal('0.0000493400000000')
    },{
        'channel': 'ticker.BTC-PERPETUAL.raw',
        'exch_ts_sec': 1787738639,
        'exch_ts_micro': 996999,
        'sys_ts_sec': 1787738645,
        'sys_ts_micro': 86210,
        'bid': Decimal('78388.00000000'),
        'ask': Decimal('78388.50000000'),
        'bid_quantity': Decimal('14410.000000000'),
        'ask_quantity': Decimal('36580.000000000'),
        'high': Decimal('79561.50000000'),
        'low': Decimal('77859.00000000'),
        'price_change': Decimal('-0.809200000000'),
        'volume': Decimal('5737.78723314000'),
        'volume_usd': Decimal('452609800.0000000000'),
        'volume_notional': Decimal('452609800.0000000000'),
        'index_price': Decimal('78358.62000000'),
        'last_price': Decimal('78389.00000000'),
        'settlement_price': Decimal('78928.85000000'),
        'min_price': Decimal('77210.00000000'),
        'max_price': Decimal('79562.00000000'),
        'open_interest': Decimal('908588760.0000000000000000'),
        'mark_price': Decimal('78385.90000000'),
        'interest_value': Decimal('0.00974420417518210777'),
        'current_funding': Decimal('0.000098140000000000006'),
        'estimated_delivery_price': Decimal('78358.619999999995'),
        'funding_8h': Decimal('0.0000493400000000')
    }]

    assert normalise_list_of_data == model_list_of_normalised_data

# ------------------------------------------------------------------------
