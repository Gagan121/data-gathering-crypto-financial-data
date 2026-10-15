import copy
import time
import re
import pytest
from core.complex_exchanges.polymarket_adapter import PolymarketAdapter, PolymarketConfig
from core.manager.polymarket_manager import PolymarketManager
from core.pipeline.streampipeline import StreamPipeline
from core.rest_requests.rest_client_requests import RestClient
from decimal import Decimal
import json
import gzip
from core.websockets.websocket_client import WebsocketClient
import asyncio
import math
from pathlib import Path
import shutil

@pytest.fixture
def polymarket_adapter_config():
    return PolymarketConfig(
        limit_number_of_channels=99,
        base_url="https://gamma-api.polymarket.com/events/keyset",
        msg={
            "assets_ids": ["<clobTokenIds>"],
            "type": "market",
            "level": 2
        },
        websocket_url="wss://ws-subscriptions-clob.polymarket.com/ws/market",
        exchange_name="TEST_Polymarket",
        interval_type="raw",
        series_id="10684",
    )


@pytest.fixture
def polymarket_adapter(polymarket_adapter_config):
    return PolymarketAdapter(
        base_url=polymarket_adapter_config.base_url,
        exchange_info={},
        channels=[],
        exchange_name=polymarket_adapter_config.exchange_name,
        websocket_url=polymarket_adapter_config.websocket_url,
        msg=polymarket_adapter_config.msg,
        heart_beat_msg={},
        heart_beat_reply_msg={}
    )


@pytest.fixture
def model_data_book_initial():
    return [{'market': '0xd37a02d8e4f5340577add930531b51bbac184eacfff13132cb869ed663194ae4',
             'asset_id': '92365246247895914650049242136497506323541732213680451262412888117420267760503',
             'timestamp': '1791548914987', 'hash': 'e6a73b95f4afa6932cbcbf77e936b65268da7b60', 'bids': [], 'asks': [],
             'tick_size': '0.01', 'event_type': 'book', 'last_trade_price': ''},
            {'market': '0xd37a02d8e4f5340577add930531b51bbac184eacfff13132cb869ed663194ae4',
             'asset_id': '24732826355128432486815107973143652972809090770361689526737953099368890068252',
             'timestamp': '1791548914987', 'hash': 'f361777a80bd6beb33933aeb176d938f0f6c83c8', 'bids': [], 'asks': [],
             'tick_size': '0.01', 'event_type': 'book', 'last_trade_price': ''},
            {'market': '0xce569bc0fd8943a763c0d6d5c20243016a1fe351512df602c0a13494f2f807d6',
             'asset_id': '53375630430312758298274311710006323375813546254947786063753569905812098280856',
             'timestamp': '1791549168270', 'hash': '68cce10e3b675a8a5f305443f200831dd4cabfa2',
             'bids': [{'price': '0.01', 'size': '100471.46'}, {'price': '0.02', 'size': '16922.2'},
                      {'price': '0.03', 'size': '11291.01'}, {'price': '0.04', 'size': '852.85'},
                      {'price': '0.05', 'size': '6934.13'}, {'price': '0.06', 'size': '516.01'},
                      {'price': '0.07', 'size': '501.01'}, {'price': '0.08', 'size': '501.01'},
                      {'price': '0.09', 'size': '501.01'}, {'price': '0.1', 'size': '3010'},
                      {'price': '0.11', 'size': '11.72'}, {'price': '0.15', 'size': '2178.75'},
                      {'price': '0.18', 'size': '5'}, {'price': '0.22', 'size': '10'},
                      {'price': '0.23', 'size': '1030'}, {'price': '0.24', 'size': '10'},
                      {'price': '0.25', 'size': '312.5'}, {'price': '0.26', 'size': '200'},
                      {'price': '0.27', 'size': '210'}, {'price': '0.28', 'size': '720'},
                      {'price': '0.29', 'size': '320'}, {'price': '0.3', 'size': '320'},
                      {'price': '0.33', 'size': '430'}, {'price': '0.35', 'size': '75'},
                      {'price': '0.36', 'size': '360'}, {'price': '0.38', 'size': '159'},
                      {'price': '0.39', 'size': '145'}, {'price': '0.4', 'size': '493'},
                      {'price': '0.41', 'size': '250'}, {'price': '0.42', 'size': '238'},
                      {'price': '0.43', 'size': '235'}, {'price': '0.44', 'size': '373'},
                      {'price': '0.45', 'size': '200'}, {'price': '0.46', 'size': '189'},
                      {'price': '0.47', 'size': '467'}, {'price': '0.48', 'size': '196'},
                      {'price': '0.49', 'size': '559.77'}, {'price': '0.5', 'size': '35'}],
             'asks': [{'price': '0.99', 'size': '99444.95'}, {'price': '0.98', 'size': '16914.21'},
                      {'price': '0.97', 'size': '11281.01'}, {'price': '0.96', 'size': '796.01'},
                      {'price': '0.95', 'size': '6939.4'}, {'price': '0.94', 'size': '516.01'},
                      {'price': '0.93', 'size': '501.01'}, {'price': '0.92', 'size': '501.01'},
                      {'price': '0.91', 'size': '501.01'}, {'price': '0.9', 'size': '3000'},
                      {'price': '0.89', 'size': '15'}, {'price': '0.88', 'size': '5.84'},
                      {'price': '0.85', 'size': '2168.75'}, {'price': '0.82', 'size': '5'},
                      {'price': '0.77', 'size': '1020'}, {'price': '0.75', 'size': '312.5'},
                      {'price': '0.72', 'size': '520'}, {'price': '0.71', 'size': '320'},
                      {'price': '0.7', 'size': '320'}, {'price': '0.69', 'size': '200'},
                      {'price': '0.67', 'size': '420'}, {'price': '0.65', 'size': '75'},
                      {'price': '0.64', 'size': '360'}, {'price': '0.62', 'size': '149'},
                      {'price': '0.61', 'size': '145'}, {'price': '0.6', 'size': '493'},
                      {'price': '0.59', 'size': '250'}, {'price': '0.58', 'size': '228'},
                      {'price': '0.57', 'size': '225'}, {'price': '0.56', 'size': '363'},
                      {'price': '0.55', 'size': '200'}, {'price': '0.54', 'size': '189'},
                      {'price': '0.53', 'size': '467'}, {'price': '0.52', 'size': '196'},
                      {'price': '0.51', 'size': '569.77'}], 'tick_size': '0.01', 'event_type': 'book',
             'last_trade_price': ''}, {'market': '0xce569bc0fd8943a763c0d6d5c20243016a1fe351512df602c0a13494f2f807d6',
                                       'asset_id': '12436205797886312193596951766498729222717998136132387193192101446751184826996',
                                       'timestamp': '1791549168270', 'hash': 'e0c39aa73e87d72579832dacb35683a800fd14e8',
                                       'bids': [{'price': '0.01', 'size': '99444.95'},
                                                {'price': '0.02', 'size': '16914.21'},
                                                {'price': '0.03', 'size': '11281.01'},
                                                {'price': '0.04', 'size': '796.01'},
                                                {'price': '0.05', 'size': '6939.4'},
                                                {'price': '0.06', 'size': '516.01'},
                                                {'price': '0.07', 'size': '501.01'},
                                                {'price': '0.08', 'size': '501.01'},
                                                {'price': '0.09', 'size': '501.01'}, {'price': '0.1', 'size': '3000'},
                                                {'price': '0.11', 'size': '15'}, {'price': '0.12', 'size': '5.84'},
                                                {'price': '0.15', 'size': '2168.75'}, {'price': '0.18', 'size': '5'},
                                                {'price': '0.23', 'size': '1020'}, {'price': '0.25', 'size': '312.5'},
                                                {'price': '0.28', 'size': '520'}, {'price': '0.29', 'size': '320'},
                                                {'price': '0.3', 'size': '320'}, {'price': '0.31', 'size': '200'},
                                                {'price': '0.33', 'size': '420'}, {'price': '0.35', 'size': '75'},
                                                {'price': '0.36', 'size': '360'}, {'price': '0.38', 'size': '149'},
                                                {'price': '0.39', 'size': '145'}, {'price': '0.4', 'size': '493'},
                                                {'price': '0.41', 'size': '250'}, {'price': '0.42', 'size': '228'},
                                                {'price': '0.43', 'size': '225'}, {'price': '0.44', 'size': '363'},
                                                {'price': '0.45', 'size': '200'}, {'price': '0.46', 'size': '189'},
                                                {'price': '0.47', 'size': '467'}, {'price': '0.48', 'size': '196'},
                                                {'price': '0.49', 'size': '569.77'}],
                                       'asks': [{'price': '0.99', 'size': '100471.46'},
                                                {'price': '0.98', 'size': '16922.2'},
                                                {'price': '0.97', 'size': '11291.01'},
                                                {'price': '0.96', 'size': '852.85'},
                                                {'price': '0.95', 'size': '6934.13'},
                                                {'price': '0.94', 'size': '516.01'},
                                                {'price': '0.93', 'size': '501.01'},
                                                {'price': '0.92', 'size': '501.01'},
                                                {'price': '0.91', 'size': '501.01'}, {'price': '0.9', 'size': '3010'},
                                                {'price': '0.89', 'size': '11.72'},
                                                {'price': '0.85', 'size': '2178.75'}, {'price': '0.82', 'size': '5'},
                                                {'price': '0.78', 'size': '10'}, {'price': '0.77', 'size': '1030'},
                                                {'price': '0.76', 'size': '10'}, {'price': '0.75', 'size': '312.5'},
                                                {'price': '0.74', 'size': '200'}, {'price': '0.73', 'size': '210'},
                                                {'price': '0.72', 'size': '720'}, {'price': '0.71', 'size': '320'},
                                                {'price': '0.7', 'size': '320'}, {'price': '0.67', 'size': '430'},
                                                {'price': '0.65', 'size': '75'}, {'price': '0.64', 'size': '360'},
                                                {'price': '0.62', 'size': '159'}, {'price': '0.61', 'size': '145'},
                                                {'price': '0.6', 'size': '493'}, {'price': '0.59', 'size': '250'},
                                                {'price': '0.58', 'size': '238'}, {'price': '0.57', 'size': '235'},
                                                {'price': '0.56', 'size': '373'}, {'price': '0.55', 'size': '200'},
                                                {'price': '0.54', 'size': '189'}, {'price': '0.53', 'size': '467'},
                                                {'price': '0.52', 'size': '196'}, {'price': '0.51', 'size': '559.77'},
                                                {'price': '0.5', 'size': '35'}], 'tick_size': '0.01',
                                       'event_type': 'book', 'last_trade_price': ''},
            {'market': '0x706a9a416bab9d8bc65b780a1fdd5297cbfd78b1f8a6c91b888bb1b2b45d4646',
             'asset_id': '61024487497240590399322467351097603162056598293957747867990027404577261185806',
             'timestamp': '1791549160499', 'hash': '7083c6f5d0dbf95682b13fcd0a5fe2f008db729e',
             'bids': [{'price': '0.01', 'size': '100302.33'}, {'price': '0.02', 'size': '16901.52'},
                      {'price': '0.03', 'size': '11331.01'}, {'price': '0.04', 'size': '836.01'},
                      {'price': '0.05', 'size': '6929.13'}, {'price': '0.06', 'size': '526.01'},
                      {'price': '0.07', 'size': '511.01'}, {'price': '0.08', 'size': '511.01'},
                      {'price': '0.09', 'size': '511.01'}, {'price': '0.1', 'size': '3010'},
                      {'price': '0.11', 'size': '10'}, {'price': '0.12', 'size': '10'}, {'price': '0.13', 'size': '10'},
                      {'price': '0.14', 'size': '10'}, {'price': '0.15', 'size': '2178.75'},
                      {'price': '0.16', 'size': '10'}, {'price': '0.18', 'size': '15'}, {'price': '0.2', 'size': '10'},
                      {'price': '0.21', 'size': '10'}, {'price': '0.22', 'size': '10'}, {'price': '0.23', 'size': '10'},
                      {'price': '0.24', 'size': '10'}, {'price': '0.25', 'size': '122.5'},
                      {'price': '0.26', 'size': '210'}, {'price': '0.27', 'size': '10'},
                      {'price': '0.28', 'size': '10'}, {'price': '0.29', 'size': '330'},
                      {'price': '0.3', 'size': '330'}, {'price': '0.31', 'size': '210'},
                      {'price': '0.32', 'size': '10'}, {'price': '0.33', 'size': '10'}, {'price': '0.34', 'size': '10'},
                      {'price': '0.35', 'size': '85'}, {'price': '0.37', 'size': '10'},
                      {'price': '0.38', 'size': '159'}, {'price': '0.39', 'size': '155'},
                      {'price': '0.4', 'size': '263'}, {'price': '0.41', 'size': '260'},
                      {'price': '0.42', 'size': '238'}, {'price': '0.43', 'size': '225'},
                      {'price': '0.44', 'size': '213'}, {'price': '0.45', 'size': '210'},
                      {'price': '0.46', 'size': '199'}, {'price': '0.47', 'size': '387'},
                      {'price': '0.48', 'size': '201'}, {'price': '0.49', 'size': '559.77'},
                      {'price': '0.5', 'size': '35'}],
             'asks': [{'price': '0.99', 'size': '100275.85'}, {'price': '0.98', 'size': '16901.27'},
                      {'price': '0.97', 'size': '11331.01'}, {'price': '0.96', 'size': '786.01'},
                      {'price': '0.95', 'size': '6929.13'}, {'price': '0.94', 'size': '526.01'},
                      {'price': '0.93', 'size': '511.01'}, {'price': '0.92', 'size': '511.01'},
                      {'price': '0.91', 'size': '511.01'}, {'price': '0.9', 'size': '3010'},
                      {'price': '0.89', 'size': '10'}, {'price': '0.88', 'size': '10'}, {'price': '0.87', 'size': '10'},
                      {'price': '0.86', 'size': '10'}, {'price': '0.85', 'size': '2178.75'},
                      {'price': '0.84', 'size': '10'}, {'price': '0.82', 'size': '15'}, {'price': '0.8', 'size': '10'},
                      {'price': '0.79', 'size': '10'}, {'price': '0.78', 'size': '10'}, {'price': '0.77', 'size': '10'},
                      {'price': '0.76', 'size': '10'}, {'price': '0.75', 'size': '322.5'},
                      {'price': '0.74', 'size': '10'}, {'price': '0.73', 'size': '10'},
                      {'price': '0.72', 'size': '210'}, {'price': '0.71', 'size': '330'},
                      {'price': '0.7', 'size': '330'}, {'price': '0.69', 'size': '210'},
                      {'price': '0.68', 'size': '10'}, {'price': '0.67', 'size': '10'}, {'price': '0.66', 'size': '10'},
                      {'price': '0.65', 'size': '85'}, {'price': '0.63', 'size': '10'},
                      {'price': '0.62', 'size': '159'}, {'price': '0.61', 'size': '155'},
                      {'price': '0.6', 'size': '263'}, {'price': '0.59', 'size': '260'},
                      {'price': '0.58', 'size': '238'}, {'price': '0.57', 'size': '225'},
                      {'price': '0.56', 'size': '213'}, {'price': '0.55', 'size': '210'},
                      {'price': '0.54', 'size': '199'}, {'price': '0.53', 'size': '387'},
                      {'price': '0.52', 'size': '201'}, {'price': '0.51', 'size': '569.77'}], 'tick_size': '0.01',
             'event_type': 'book', 'last_trade_price': ''},
            {'market': '0x706a9a416bab9d8bc65b780a1fdd5297cbfd78b1f8a6c91b888bb1b2b45d4646',
             'asset_id': '80787442587856751219701037219674284427871157182176559743196374119442676346453',
             'timestamp': '1791549160499', 'hash': '1a74f83e0d070d483a18d3fb4b569279e7437925',
             'bids': [{'price': '0.01', 'size': '100275.85'}, {'price': '0.02', 'size': '16901.27'},
                      {'price': '0.03', 'size': '11331.01'}, {'price': '0.04', 'size': '786.01'},
                      {'price': '0.05', 'size': '6929.13'}, {'price': '0.06', 'size': '526.01'},
                      {'price': '0.07', 'size': '511.01'}, {'price': '0.08', 'size': '511.01'},
                      {'price': '0.09', 'size': '511.01'}, {'price': '0.1', 'size': '3010'},
                      {'price': '0.11', 'size': '10'}, {'price': '0.12', 'size': '10'}, {'price': '0.13', 'size': '10'},
                      {'price': '0.14', 'size': '10'}, {'price': '0.15', 'size': '2178.75'},
                      {'price': '0.16', 'size': '10'}, {'price': '0.18', 'size': '15'}, {'price': '0.2', 'size': '10'},
                      {'price': '0.21', 'size': '10'}, {'price': '0.22', 'size': '10'}, {'price': '0.23', 'size': '10'},
                      {'price': '0.24', 'size': '10'}, {'price': '0.25', 'size': '322.5'},
                      {'price': '0.26', 'size': '10'}, {'price': '0.27', 'size': '10'},
                      {'price': '0.28', 'size': '210'}, {'price': '0.29', 'size': '330'},
                      {'price': '0.3', 'size': '330'}, {'price': '0.31', 'size': '210'},
                      {'price': '0.32', 'size': '10'}, {'price': '0.33', 'size': '10'}, {'price': '0.34', 'size': '10'},
                      {'price': '0.35', 'size': '85'}, {'price': '0.37', 'size': '10'},
                      {'price': '0.38', 'size': '159'}, {'price': '0.39', 'size': '155'},
                      {'price': '0.4', 'size': '263'}, {'price': '0.41', 'size': '260'},
                      {'price': '0.42', 'size': '238'}, {'price': '0.43', 'size': '225'},
                      {'price': '0.44', 'size': '213'}, {'price': '0.45', 'size': '210'},
                      {'price': '0.46', 'size': '199'}, {'price': '0.47', 'size': '387'},
                      {'price': '0.48', 'size': '201'}, {'price': '0.49', 'size': '569.77'}],
             'asks': [{'price': '0.99', 'size': '100302.33'}, {'price': '0.98', 'size': '16901.52'},
                      {'price': '0.97', 'size': '11331.01'}, {'price': '0.96', 'size': '836.01'},
                      {'price': '0.95', 'size': '6929.13'}, {'price': '0.94', 'size': '526.01'},
                      {'price': '0.93', 'size': '511.01'}, {'price': '0.92', 'size': '511.01'},
                      {'price': '0.91', 'size': '511.01'}, {'price': '0.9', 'size': '3010'},
                      {'price': '0.89', 'size': '10'}, {'price': '0.88', 'size': '10'}, {'price': '0.87', 'size': '10'},
                      {'price': '0.86', 'size': '10'}, {'price': '0.85', 'size': '2178.75'},
                      {'price': '0.84', 'size': '10'}, {'price': '0.82', 'size': '15'}, {'price': '0.8', 'size': '10'},
                      {'price': '0.79', 'size': '10'}, {'price': '0.78', 'size': '10'}, {'price': '0.77', 'size': '10'},
                      {'price': '0.76', 'size': '10'}, {'price': '0.75', 'size': '122.5'},
                      {'price': '0.74', 'size': '210'}, {'price': '0.73', 'size': '10'},
                      {'price': '0.72', 'size': '10'}, {'price': '0.71', 'size': '330'},
                      {'price': '0.7', 'size': '330'}, {'price': '0.69', 'size': '210'},
                      {'price': '0.68', 'size': '10'}, {'price': '0.67', 'size': '10'}, {'price': '0.66', 'size': '10'},
                      {'price': '0.65', 'size': '85'}, {'price': '0.63', 'size': '10'},
                      {'price': '0.62', 'size': '159'}, {'price': '0.61', 'size': '155'},
                      {'price': '0.6', 'size': '263'}, {'price': '0.59', 'size': '260'},
                      {'price': '0.58', 'size': '238'}, {'price': '0.57', 'size': '225'},
                      {'price': '0.56', 'size': '213'}, {'price': '0.55', 'size': '210'},
                      {'price': '0.54', 'size': '199'}, {'price': '0.53', 'size': '387'},
                      {'price': '0.52', 'size': '201'}, {'price': '0.51', 'size': '559.77'},
                      {'price': '0.5', 'size': '35'}], 'tick_size': '0.01', 'event_type': 'book',
             'last_trade_price': ''}, {'market': '0x12ceccda15598549aa70da45ed0ff62e5d657cb59e5ac907a6e008c5dbb8a8fa',
                                       'asset_id': '73432345403558778216509911841506585248627348303948893840311045405335993091482',
                                       'timestamp': '1791549160071', 'hash': '4242a6dbbe44c827abd170df2834d0a412ed7928',
                                       'bids': [{'price': '0.01', 'size': '99973.53'},
                                                {'price': '0.02', 'size': '16971.63'},
                                                {'price': '0.03', 'size': '11316.01'},
                                                {'price': '0.04', 'size': '856.01'},
                                                {'price': '0.05', 'size': '6934.13'},
                                                {'price': '0.06', 'size': '516.01'},
                                                {'price': '0.07', 'size': '501.01'},
                                                {'price': '0.08', 'size': '501.01'},
                                                {'price': '0.09', 'size': '501.01'}, {'price': '0.1', 'size': '3010'},
                                                {'price': '0.11', 'size': '10'}, {'price': '0.12', 'size': '10'},
                                                {'price': '0.13', 'size': '10'}, {'price': '0.15', 'size': '2178.75'},
                                                {'price': '0.17', 'size': '10'}, {'price': '0.18', 'size': '10'},
                                                {'price': '0.2', 'size': '10'}, {'price': '0.21', 'size': '10'},
                                                {'price': '0.22', 'size': '10'}, {'price': '0.25', 'size': '322.5'},
                                                {'price': '0.26', 'size': '10'}, {'price': '0.27', 'size': '10'},
                                                {'price': '0.28', 'size': '200'}, {'price': '0.29', 'size': '330'},
                                                {'price': '0.3', 'size': '330'}, {'price': '0.31', 'size': '210'},
                                                {'price': '0.33', 'size': '10'}, {'price': '0.34', 'size': '10'},
                                                {'price': '0.35', 'size': '75'}, {'price': '0.37', 'size': '10'},
                                                {'price': '0.38', 'size': '149'}, {'price': '0.39', 'size': '155'},
                                                {'price': '0.4', 'size': '253'}, {'price': '0.41', 'size': '260'},
                                                {'price': '0.42', 'size': '238'}, {'price': '0.43', 'size': '225'},
                                                {'price': '0.44', 'size': '213'}, {'price': '0.45', 'size': '210'},
                                                {'price': '0.46', 'size': '189'}, {'price': '0.47', 'size': '387'},
                                                {'price': '0.48', 'size': '196'}, {'price': '0.49', 'size': '560.19'},
                                                {'price': '0.5', 'size': '30'}],
                                       'asks': [{'price': '0.99', 'size': '99946.81'},
                                                {'price': '0.98', 'size': '16971.1'},
                                                {'price': '0.97', 'size': '11316.01'},
                                                {'price': '0.96', 'size': '806.01'},
                                                {'price': '0.95', 'size': '6934.13'},
                                                {'price': '0.94', 'size': '516.01'},
                                                {'price': '0.93', 'size': '501.01'},
                                                {'price': '0.92', 'size': '501.01'},
                                                {'price': '0.91', 'size': '501.01'}, {'price': '0.9', 'size': '3010'},
                                                {'price': '0.89', 'size': '10'}, {'price': '0.88', 'size': '10'},
                                                {'price': '0.87', 'size': '10'}, {'price': '0.85', 'size': '2178.75'},
                                                {'price': '0.83', 'size': '10'}, {'price': '0.82', 'size': '10'},
                                                {'price': '0.8', 'size': '10'}, {'price': '0.79', 'size': '10'},
                                                {'price': '0.78', 'size': '10'}, {'price': '0.75', 'size': '322.5'},
                                                {'price': '0.74', 'size': '210'}, {'price': '0.73', 'size': '210'},
                                                {'price': '0.71', 'size': '330'}, {'price': '0.7', 'size': '330'},
                                                {'price': '0.69', 'size': '10'}, {'price': '0.67', 'size': '10'},
                                                {'price': '0.66', 'size': '10'}, {'price': '0.65', 'size': '75'},
                                                {'price': '0.63', 'size': '10'}, {'price': '0.62', 'size': '149'},
                                                {'price': '0.61', 'size': '155'}, {'price': '0.6', 'size': '253'},
                                                {'price': '0.59', 'size': '260'}, {'price': '0.58', 'size': '238'},
                                                {'price': '0.57', 'size': '225'}, {'price': '0.56', 'size': '213'},
                                                {'price': '0.55', 'size': '210'}, {'price': '0.54', 'size': '189'},
                                                {'price': '0.53', 'size': '387'}, {'price': '0.52', 'size': '196'},
                                                {'price': '0.51', 'size': '565.19'}], 'tick_size': '0.01',
                                       'event_type': 'book', 'last_trade_price': ''},
            {'market': '0x12ceccda15598549aa70da45ed0ff62e5d657cb59e5ac907a6e008c5dbb8a8fa',
             'asset_id': '9510719965850420024799447271914145758557425424360732340799488076869718606039',
             'timestamp': '1791549160071', 'hash': 'f0b337fc019bd32a6555f181022a8629b11fce55',
             'bids': [{'price': '0.01', 'size': '99946.81'}, {'price': '0.02', 'size': '16971.1'},
                      {'price': '0.03', 'size': '11316.01'}, {'price': '0.04', 'size': '806.01'},
                      {'price': '0.05', 'size': '6934.13'}, {'price': '0.06', 'size': '516.01'},
                      {'price': '0.07', 'size': '501.01'}, {'price': '0.08', 'size': '501.01'},
                      {'price': '0.09', 'size': '501.01'}, {'price': '0.1', 'size': '3010'},
                      {'price': '0.11', 'size': '10'}, {'price': '0.12', 'size': '10'}, {'price': '0.13', 'size': '10'},
                      {'price': '0.15', 'size': '2178.75'}, {'price': '0.17', 'size': '10'},
                      {'price': '0.18', 'size': '10'}, {'price': '0.2', 'size': '10'}, {'price': '0.21', 'size': '10'},
                      {'price': '0.22', 'size': '10'}, {'price': '0.25', 'size': '322.5'},
                      {'price': '0.26', 'size': '210'}, {'price': '0.27', 'size': '210'},
                      {'price': '0.29', 'size': '330'}, {'price': '0.3', 'size': '330'},
                      {'price': '0.31', 'size': '10'}, {'price': '0.33', 'size': '10'}, {'price': '0.34', 'size': '10'},
                      {'price': '0.35', 'size': '75'}, {'price': '0.37', 'size': '10'},
                      {'price': '0.38', 'size': '149'}, {'price': '0.39', 'size': '155'},
                      {'price': '0.4', 'size': '253'}, {'price': '0.41', 'size': '260'},
                      {'price': '0.42', 'size': '238'}, {'price': '0.43', 'size': '225'},
                      {'price': '0.44', 'size': '213'}, {'price': '0.45', 'size': '210'},
                      {'price': '0.46', 'size': '189'}, {'price': '0.47', 'size': '387'},
                      {'price': '0.48', 'size': '196'}, {'price': '0.49', 'size': '565.19'}],
             'asks': [{'price': '0.99', 'size': '99973.53'}, {'price': '0.98', 'size': '16971.63'},
                      {'price': '0.97', 'size': '11316.01'}, {'price': '0.96', 'size': '856.01'},
                      {'price': '0.95', 'size': '6934.13'}, {'price': '0.94', 'size': '516.01'},
                      {'price': '0.93', 'size': '501.01'}, {'price': '0.92', 'size': '501.01'},
                      {'price': '0.91', 'size': '501.01'}, {'price': '0.9', 'size': '3010'},
                      {'price': '0.89', 'size': '10'}, {'price': '0.88', 'size': '10'}, {'price': '0.87', 'size': '10'},
                      {'price': '0.85', 'size': '2178.75'}, {'price': '0.83', 'size': '10'},
                      {'price': '0.82', 'size': '10'}, {'price': '0.8', 'size': '10'}, {'price': '0.79', 'size': '10'},
                      {'price': '0.78', 'size': '10'}, {'price': '0.75', 'size': '322.5'},
                      {'price': '0.74', 'size': '10'}, {'price': '0.73', 'size': '10'},
                      {'price': '0.72', 'size': '200'}, {'price': '0.71', 'size': '330'},
                      {'price': '0.7', 'size': '330'}, {'price': '0.69', 'size': '210'},
                      {'price': '0.67', 'size': '10'}, {'price': '0.66', 'size': '10'}, {'price': '0.65', 'size': '75'},
                      {'price': '0.63', 'size': '10'}, {'price': '0.62', 'size': '149'},
                      {'price': '0.61', 'size': '155'}, {'price': '0.6', 'size': '253'},
                      {'price': '0.59', 'size': '260'}, {'price': '0.58', 'size': '238'},
                      {'price': '0.57', 'size': '225'}, {'price': '0.56', 'size': '213'},
                      {'price': '0.55', 'size': '210'}, {'price': '0.54', 'size': '189'},
                      {'price': '0.53', 'size': '387'}, {'price': '0.52', 'size': '196'},
                      {'price': '0.51', 'size': '560.19'}, {'price': '0.5', 'size': '30'}], 'tick_size': '0.01',
             'event_type': 'book', 'last_trade_price': ''}, {'sys_time': 1791549192.3634286}]


@pytest.fixture
def model_data_book_secondary():
    return {'market': '0x20c2c0c2d897d1bf2116f258ca7104fe9e5e3ae97d9c4ea2b0f4f70078a51544',
            'asset_id': '1316211778311329685045645979366681620182409688329148910713383446531301807652',
            'bids': [{'price': '0.01', 'size': '8489.25'}, {'price': '0.02', 'size': '3028.46'},
                     {'price': '0.03', 'size': '5047.37'}, {'price': '0.04', 'size': '1043.08'},
                     {'price': '0.05', 'size': '1003.55'}, {'price': '0.06', 'size': '1025.88'},
                     {'price': '0.07', 'size': '796.43'}, {'price': '0.08', 'size': '683.51'},
                     {'price': '0.09', 'size': '754.56'}, {'price': '0.1', 'size': '181'},
                     {'price': '0.11', 'size': '224'}, {'price': '0.12', 'size': '276.32'},
                     {'price': '0.13', 'size': '140'}, {'price': '0.14', 'size': '68'},
                     {'price': '0.15', 'size': '275.98'}, {'price': '0.16', 'size': '262'},
                     {'price': '0.17', 'size': '498.01'}, {'price': '0.18', 'size': '158'},
                     {'price': '0.19', 'size': '142.31'}, {'price': '0.2', 'size': '293'},
                     {'price': '0.21', 'size': '150'}, {'price': '0.22', 'size': '138'},
                     {'price': '0.23', 'size': '143'}, {'price': '0.24', 'size': '136'},
                     {'price': '0.25', 'size': '169'}, {'price': '0.26', 'size': '143'},
                     {'price': '0.27', 'size': '132'}, {'price': '0.28', 'size': '105'},
                     {'price': '0.29', 'size': '1634'}, {'price': '0.3', 'size': '121.36'},
                     {'price': '0.31', 'size': '59.33'}, {'price': '0.32', 'size': '30'},
                     {'price': '0.33', 'size': '30'}, {'price': '0.34', 'size': '65'},
                     {'price': '0.35', 'size': '72.24'}],
            'asks': [{'price': '0.99', 'size': '8579.64'}, {'price': '0.98', 'size': '2973.46'},
                     {'price': '0.97', 'size': '3620.43'}, {'price': '0.96', 'size': '8428.08'},
                     {'price': '0.95', 'size': '7128.55'}, {'price': '0.94', 'size': '1390.88'},
                     {'price': '0.93', 'size': '2790'}, {'price': '0.92', 'size': '1298.51'},
                     {'price': '0.91', 'size': '1329.56'}, {'price': '0.9', 'size': '658'},
                     {'price': '0.89', 'size': '662.53'}, {'price': '0.88', 'size': '89'},
                     {'price': '0.87', 'size': '1647'}, {'price': '0.86', 'size': '73'},
                     {'price': '0.85', 'size': '132'}, {'price': '0.84', 'size': '269'},
                     {'price': '0.83', 'size': '73'}, {'price': '0.82', 'size': '65'}, {'price': '0.81', 'size': '75'},
                     {'price': '0.8', 'size': '1222.9'}, {'price': '0.79', 'size': '173'},
                     {'price': '0.78', 'size': '60'}, {'price': '0.77', 'size': '192'}, {'price': '0.76', 'size': '59'},
                     {'price': '0.75', 'size': '74'}, {'price': '0.74', 'size': '157'}, {'price': '0.73', 'size': '67'},
                     {'price': '0.72', 'size': '56'}, {'price': '0.71', 'size': '41'}, {'price': '0.7', 'size': '733'},
                     {'price': '0.69', 'size': '156'}, {'price': '0.68', 'size': '64'}, {'price': '0.67', 'size': '56'},
                     {'price': '0.66', 'size': '88'}, {'price': '0.65', 'size': '101'},
                     {'price': '0.64', 'size': '177'}, {'price': '0.63', 'size': '66'}, {'price': '0.62', 'size': '57'},
                     {'price': '0.61', 'size': '46'}, {'price': '0.6', 'size': '343.99'},
                     {'price': '0.59', 'size': '157'}, {'price': '0.58', 'size': '228'},
                     {'price': '0.57', 'size': '99'}, {'price': '0.56', 'size': '301'},
                     {'price': '0.55', 'size': '386'}, {'price': '0.54', 'size': '295'},
                     {'price': '0.53', 'size': '294'}, {'price': '0.52', 'size': '151'},
                     {'price': '0.51', 'size': '168.01'}, {'price': '0.5', 'size': '175'},
                     {'price': '0.49', 'size': '298'}, {'price': '0.48', 'size': '299'},
                     {'price': '0.47', 'size': '309'}, {'price': '0.46', 'size': '335'},
                     {'price': '0.45', 'size': '411'}, {'price': '0.44', 'size': '110'},
                     {'price': '0.43', 'size': '160'}, {'price': '0.42', 'size': '199'},
                     {'price': '0.41', 'size': '241'}, {'price': '0.4', 'size': '636'},
                     {'price': '0.39', 'size': '1105'}, {'price': '0.38', 'size': '480.62'},
                     {'price': '0.37', 'size': '333.7'}, {'price': '0.36', 'size': '1605.61'}],
            'hash': 'f5b4ea10031d1d53d17985e147352f21e59f0470', 'timestamp': '1791627916999', 'event_type': 'book',
            'sys_time': 1791627916.902125}


@pytest.fixture
def model_data_price_changes():
    return {'market': '0xd37a02d8e4f5340577add930531b51bbac184eacfff13132cb869ed663194ae4', 'price_changes': [
        {'asset_id': '92365246247895914650049242136497506323541732213680451262412888117420267760503', 'price': '0.49',
         'size': '117', 'side': 'BUY', 'hash': '4344e1e80ebb6ebe95a8fb63971fa31d79a87b0e', 'best_bid': '0.49',
         'best_ask': '1'},
        {'asset_id': '24732826355128432486815107973143652972809090770361689526737953099368890068252', 'price': '0.51',
         'size': '117', 'side': 'SELL', 'hash': 'a41adae811e4c58407c22c5c19e19b5f50c5b18a', 'best_bid': '0',
         'best_ask': '0.51'}], 'timestamp': '1791549207492', 'event_type': 'price_change',
            'sys_time': 1791549381.2840257}


@pytest.fixture
def model_data_tick_size_changes():
    return {'market': '0xfac1c6c31fc896220699b8f2ccad0459f690f5b98a7d6f7cfcac999e7bfc48ed',
            'asset_id': '42215505728357508195209790760712417278627646406240537861497755485097801777900',
            'old_tick_size': '0.01', 'new_tick_size': '0.001', 'timestamp': '1791627334385',
            'event_type': 'tick_size_change', 'sys_time': 1791627334.2894661}


@pytest.fixture
def model_data_last_trade_price():
    return {'market': '0x1fd1459480c91ba91040665fe905f9b913e12d49cb0c345f72f7a7b0dd7d4e23',
            'asset_id': '31403232306013778162476061765940015949835322748651947415662658615459066883162',
            'price': '0.74', 'size': '1.35135', 'fee_rate_bps': '0', 'side': 'BUY', 'timestamp': '1791627513118',
            'event_type': 'last_trade_price',
            'transaction_hash': '0xf0c5f60115b5837c38756842f653af811201abc760e1a7064fd53408702f0391',
            'sys_time': 1791627709.034008}


@pytest.fixture
def provide_event_info():
    return  {"92365246247895914650049242136497506323541732213680451262412888117420267760503" :{'id': '1182986', 'ticker': 'btc-updown-5m-1791987600', 'slug': 'btc-updown-5m-1791987600',
            'title': 'Bitcoin Up or Down - October 14, 10:20AM-10:25AM ET',
            'description': 'This market will resolve to "Up" if the time-weighted average price (TWAP) of Bitcoin, generated by Chainlink, of the time range specified in the title is greater than or equal to the price at the beginning of that range. Otherwise, it will resolve to "Down".\nThe resolution source for this market is information from Chainlink, specifically the BTC/USD TWAP data stream available at https://data.chain.link/streams/btc-usd-twap-60s-streams.\nPlease note that this market is about the price according to the TWAP Chainlink data stream for the asset pair BTC/USD, not according to any other sources or spot markets.',
            'resolutionSource': 'https://data.chain.link/streams/btc-usd-twap-60s-streams',
            'startDate': '2026-10-13T14:28:53.490577Z', 'creationDate': '2026-10-13T14:29:37.141434Z',
            'endDate': '2026-10-14T14:25:00Z',
            'image': 'https://polymarket-upload.s3.us-east-2.amazonaws.com/BTC+fullsize.png',
            'icon': 'https://polymarket-upload.s3.us-east-2.amazonaws.com/BTC+fullsize.png', 'active': True,
            'closed': False, 'archived': False, 'new': False, 'featured': False, 'restricted': True,
            'liquidity': 19860.4586, 'volume': 2, 'openInterest': 4, 'createdAt': '2026-10-13T14:27:39.378692Z',
            'updatedAt': '2026-10-14T14:19:01.580197Z', 'competitive': 0.9999750006249843, 'volume24hr': 2,
            'volume1wk': 2, 'volume1mo': 2, 'volume1yr': 2, 'enableOrderBook': True, 'liquidityClob': 19860.4586,
            'negRisk': False, 'commentCount': 0, 'markets': [
            {'id': '5649599', 'question': 'Bitcoin Up or Down - October 14, 10:20AM-10:25AM ET',
             'conditionId': '0x22c01db5804f915eadd7ac09c6d988fdc549e5bb4365a359606b22499b00ef37',
             'slug': 'btc-updown-5m-1791987600',
             'resolutionSource': 'https://data.chain.link/streams/btc-usd-twap-60s-streams',
             'endDate': '2026-10-14T14:25:00Z', 'liquidity': '20183.4661', 'startDate': '2026-10-13T14:28:53.490577Z',
             'image': 'https://polymarket-upload.s3.us-east-2.amazonaws.com/BTC+fullsize.png',
             'icon': 'https://polymarket-upload.s3.us-east-2.amazonaws.com/BTC+fullsize.png',
             'description': 'This market will resolve to "Up" if the time-weighted average price (TWAP) of Bitcoin, generated by Chainlink, of the time range specified in the title is greater than or equal to the price at the beginning of that range. Otherwise, it will resolve to "Down".\nThe resolution source for this market is information from Chainlink, specifically the BTC/USD TWAP data stream available at https://data.chain.link/streams/btc-usd-twap-60s-streams.\nPlease note that this market is about the price according to the TWAP Chainlink data stream for the asset pair BTC/USD, not according to any other sources or spot markets.',
             'outcomes': ['Up', 'Down'], 'outcomePrices': '["0.505", "0.495"]', 'volume': '2', 'active': True,
             'closed': False, 'marketMakerAddress': '', 'createdAt': '2026-10-13T14:27:39.556948Z',
             'updatedAt': '2026-10-14T14:18:46.422257Z', 'new': False, 'featured': False, 'archived': False,
             'restricted': True, 'groupItemThreshold': '0',
             'questionID': '0x58a484d0aec77c440fc8c3f4c04a5cf448263351170778d1dd82fc47b3ee8963',
             'enableOrderBook': True, 'orderPriceMinTickSize': 0.01, 'orderMinSize': 5, 'volumeNum': 2,
             'liquidityNum': 20183.4661, 'endDateIso': '2026-10-14', 'startDateIso': '2026-10-13',
             'hasReviewedDates': True, 'volume24hr': 2, 'volume1wk': 2, 'volume1mo': 2, 'volume1yr': 2,
             'clobTokenIds': ['92365246247895914650049242136497506323541732213680451262412888117420267760503',
                              '42688132215870817598716998885393186333341556900269672680007430402681499306897'],
             'comboStatus': 'disabled', 'volume24hrClob': 2, 'volume1wkClob': 2, 'volume1moClob': 2, 'volume1yrClob': 2,
             'volumeClob': 2, 'liquidityClob': 20183.4661, 'makerBaseFee': 1000, 'takerBaseFee': 1000,
             'acceptingOrders': True, 'negRisk': False, 'ready': False, 'funded': False,
             'acceptingOrdersTimestamp': '2026-10-13T14:27:50Z', 'cyom': False, 'competitive': 0.9999750006249843,
             'approved': True, 'rewardsMinSize': 50, 'rewardsMaxSpread': 4.5, 'spread': 0.01, 'lastTradePrice': 0.51,
             'bestBid': 0.5, 'bestAsk': 0.51, 'automaticallyActive': True, 'clearBookOnStart': False,
             'showGmpSeries': False, 'showGmpOutcome': False, 'manualActivation': False, 'negRiskOther': False,
             'umaResolutionStatuses': '[]', 'pendingDeployment': False, 'deploying': False, 'rfqEnabled': False,
             'eventStartTime': '2026-10-14T14:20:00Z', 'holdingRewardsEnabled': False, 'feesEnabled': True,
             'makerRebatesFeeShareBps': 10000, 'feeType': 'crypto_fees_v2',
             'feeSchedule': {'exponent': 1, 'rate': 0.07, 'takerOnly': True, 'rebateRate': 0.2},
             'cryptoMarketConfigId': 'btc-5m-twap-60',
             'cryptoMarketConfig': {'id': 'btc-5m-twap-60', 'asset': 'btc', 'duration': '5m', 'twapEnabled': True,
                                    'twapLookbackSeconds': 60}, 'version': 'v1'}], 'series': [
            {'id': '10684', 'ticker': 'btc-up-or-down-5m', 'slug': 'btc-up-or-down-5m', 'title': 'BTC Up or Down 5m',
             'seriesType': 'single', 'recurrence': '5m', 'image': '', 'icon': '', 'active': True, 'closed': False,
             'archived': False, 'featured': False, 'restricted': True, 'createdAt': '2025-11-21T21:46:22.201627Z',
             'updatedAt': '2026-10-14T14:19:16.641637Z', 'volume24hr': 8874446.915543, 'volume': 64954.080685,
             'liquidity': 1783900.32919, 'commentCount': 97568}], 'tags': [
            {'id': '102127', 'label': 'Up or Down', 'slug': 'up-or-down', 'forceShow': True,
             'createdAt': '2025-05-09T20:56:58.880785Z', 'updatedAt': '2026-03-09T22:33:38.505979Z',
             'isCarousel': False}, {'id': '1312', 'label': 'Crypto Prices', 'slug': 'crypto-prices', 'forceShow': True,
                                    'publishedAt': '2024-02-12 22:36:40.783+00', 'createdBy': 15, 'updatedBy': 15,
                                    'createdAt': '2024-02-12T22:34:50.962Z',
                                    'updatedAt': '2026-04-17T20:52:10.030771Z'},
            {'id': '102169', 'label': 'Hide From New', 'slug': 'hide-from-new', 'forceShow': False,
             'createdAt': '2025-05-23T18:47:07.430147Z', 'updatedAt': '2026-04-17T20:28:28.912197Z',
             'isCarousel': False}, {'id': '101757', 'label': 'Recurring', 'slug': 'recurring', 'forceShow': False,
                                    'createdAt': '2025-02-01T00:45:26.493838Z',
                                    'updatedAt': '2026-04-17T17:25:43.117249Z'},
            {'id': '21', 'label': 'Crypto', 'slug': 'crypto', 'forceShow': False,
             'publishedAt': '2023-11-02 21:03:54.55+00', 'updatedBy': 15, 'createdAt': '2023-11-02T21:03:54.564Z',
             'updatedAt': '2026-03-09T22:28:03.349847Z'},
            {'id': '235', 'label': 'Bitcoin', 'slug': 'bitcoin', 'forceShow': True,
             'publishedAt': '2023-11-02 21:40:32.968+00', 'updatedBy': 15, 'createdAt': '2023-11-02T21:40:32.974Z',
             'updatedAt': '2026-04-17T20:39:34.210222Z'},
            {'id': '102892', 'label': '5M', 'slug': '5M', 'forceShow': False,
             'createdAt': '2025-11-21T22:00:21.004867Z', 'updatedAt': '2026-04-17T20:48:44.301846Z',
             'isCarousel': False}], 'cyom': False, 'showAllOutcomes': True, 'showMarketImages': True,
            'enableNegRisk': False, 'automaticallyActive': True, 'startTime': '2026-10-14T14:20:00Z',
            'seriesSlug': 'btc-up-or-down-5m', 'negRiskAugmented': False, 'pendingDeployment': False,
            'deploying': False, 'version': 'v1'}
    }



@pytest.mark.asyncio
async def test_model_data_book_initial_validate(polymarket_adapter, model_data_book_initial):
    data = polymarket_adapter.valid_message_can_pass_and_restructure_data(msg=model_data_book_initial)

    assert "data" in data
    assert "valid" in data
    assert data["valid"]
    assert isinstance(data["data"], list)
    assert "channel" in data["data"][0]

    ideal_data = {'valid': True, 'data': [{'market': '0xd37a02d8e4f5340577add930531b51bbac184eacfff13132cb869ed663194ae4', 'asset_id': '92365246247895914650049242136497506323541732213680451262412888117420267760503', 'timestamp': '1791548914987', 'hash': 'e6a73b95f4afa6932cbcbf77e936b65268da7b60', 'bids': [], 'asks': [], 'tick_size': '0.01', 'event_type': 'book', 'last_trade_price': '', 'exch_ts_sec': 1791548914, 'exch_ts_micro': 986999, 'sys_ts_sec': 1791549192, 'sys_ts_micro': 363428, 'channel': '92365246247895914650049242136497506323541732213680451262412888117420267760503'}, {'market': '0xd37a02d8e4f5340577add930531b51bbac184eacfff13132cb869ed663194ae4', 'asset_id': '24732826355128432486815107973143652972809090770361689526737953099368890068252', 'timestamp': '1791548914987', 'hash': 'f361777a80bd6beb33933aeb176d938f0f6c83c8', 'bids': [], 'asks': [], 'tick_size': '0.01', 'event_type': 'book', 'last_trade_price': '', 'exch_ts_sec': 1791548914, 'exch_ts_micro': 986999, 'sys_ts_sec': 1791549192, 'sys_ts_micro': 363428, 'channel': '24732826355128432486815107973143652972809090770361689526737953099368890068252'}, {'market': '0xce569bc0fd8943a763c0d6d5c20243016a1fe351512df602c0a13494f2f807d6', 'asset_id': '53375630430312758298274311710006323375813546254947786063753569905812098280856', 'timestamp': '1791549168270', 'hash': '68cce10e3b675a8a5f305443f200831dd4cabfa2', 'bids': [{'price': '0.01', 'size': '100471.46'}, {'price': '0.02', 'size': '16922.2'}, {'price': '0.03', 'size': '11291.01'}, {'price': '0.04', 'size': '852.85'}, {'price': '0.05', 'size': '6934.13'}, {'price': '0.06', 'size': '516.01'}, {'price': '0.07', 'size': '501.01'}, {'price': '0.08', 'size': '501.01'}, {'price': '0.09', 'size': '501.01'}, {'price': '0.1', 'size': '3010'}, {'price': '0.11', 'size': '11.72'}, {'price': '0.15', 'size': '2178.75'}, {'price': '0.18', 'size': '5'}, {'price': '0.22', 'size': '10'}, {'price': '0.23', 'size': '1030'}, {'price': '0.24', 'size': '10'}, {'price': '0.25', 'size': '312.5'}, {'price': '0.26', 'size': '200'}, {'price': '0.27', 'size': '210'}, {'price': '0.28', 'size': '720'}, {'price': '0.29', 'size': '320'}, {'price': '0.3', 'size': '320'}, {'price': '0.33', 'size': '430'}, {'price': '0.35', 'size': '75'}, {'price': '0.36', 'size': '360'}, {'price': '0.38', 'size': '159'}, {'price': '0.39', 'size': '145'}, {'price': '0.4', 'size': '493'}, {'price': '0.41', 'size': '250'}, {'price': '0.42', 'size': '238'}, {'price': '0.43', 'size': '235'}, {'price': '0.44', 'size': '373'}, {'price': '0.45', 'size': '200'}, {'price': '0.46', 'size': '189'}, {'price': '0.47', 'size': '467'}, {'price': '0.48', 'size': '196'}, {'price': '0.49', 'size': '559.77'}, {'price': '0.5', 'size': '35'}], 'asks': [{'price': '0.99', 'size': '99444.95'}, {'price': '0.98', 'size': '16914.21'}, {'price': '0.97', 'size': '11281.01'}, {'price': '0.96', 'size': '796.01'}, {'price': '0.95', 'size': '6939.4'}, {'price': '0.94', 'size': '516.01'}, {'price': '0.93', 'size': '501.01'}, {'price': '0.92', 'size': '501.01'}, {'price': '0.91', 'size': '501.01'}, {'price': '0.9', 'size': '3000'}, {'price': '0.89', 'size': '15'}, {'price': '0.88', 'size': '5.84'}, {'price': '0.85', 'size': '2168.75'}, {'price': '0.82', 'size': '5'}, {'price': '0.77', 'size': '1020'}, {'price': '0.75', 'size': '312.5'}, {'price': '0.72', 'size': '520'}, {'price': '0.71', 'size': '320'}, {'price': '0.7', 'size': '320'}, {'price': '0.69', 'size': '200'}, {'price': '0.67', 'size': '420'}, {'price': '0.65', 'size': '75'}, {'price': '0.64', 'size': '360'}, {'price': '0.62', 'size': '149'}, {'price': '0.61', 'size': '145'}, {'price': '0.6', 'size': '493'}, {'price': '0.59', 'size': '250'}, {'price': '0.58', 'size': '228'}, {'price': '0.57', 'size': '225'}, {'price': '0.56', 'size': '363'}, {'price': '0.55', 'size': '200'}, {'price': '0.54', 'size': '189'}, {'price': '0.53', 'size': '467'}, {'price': '0.52', 'size': '196'}, {'price': '0.51', 'size': '569.77'}], 'tick_size': '0.01', 'event_type': 'book', 'last_trade_price': '', 'exch_ts_sec': 1791549168, 'exch_ts_micro': 269999, 'sys_ts_sec': 1791549192, 'sys_ts_micro': 363428, 'channel': '53375630430312758298274311710006323375813546254947786063753569905812098280856'}, {'market': '0xce569bc0fd8943a763c0d6d5c20243016a1fe351512df602c0a13494f2f807d6', 'asset_id': '12436205797886312193596951766498729222717998136132387193192101446751184826996', 'timestamp': '1791549168270', 'hash': 'e0c39aa73e87d72579832dacb35683a800fd14e8', 'bids': [{'price': '0.01', 'size': '99444.95'}, {'price': '0.02', 'size': '16914.21'}, {'price': '0.03', 'size': '11281.01'}, {'price': '0.04', 'size': '796.01'}, {'price': '0.05', 'size': '6939.4'}, {'price': '0.06', 'size': '516.01'}, {'price': '0.07', 'size': '501.01'}, {'price': '0.08', 'size': '501.01'}, {'price': '0.09', 'size': '501.01'}, {'price': '0.1', 'size': '3000'}, {'price': '0.11', 'size': '15'}, {'price': '0.12', 'size': '5.84'}, {'price': '0.15', 'size': '2168.75'}, {'price': '0.18', 'size': '5'}, {'price': '0.23', 'size': '1020'}, {'price': '0.25', 'size': '312.5'}, {'price': '0.28', 'size': '520'}, {'price': '0.29', 'size': '320'}, {'price': '0.3', 'size': '320'}, {'price': '0.31', 'size': '200'}, {'price': '0.33', 'size': '420'}, {'price': '0.35', 'size': '75'}, {'price': '0.36', 'size': '360'}, {'price': '0.38', 'size': '149'}, {'price': '0.39', 'size': '145'}, {'price': '0.4', 'size': '493'}, {'price': '0.41', 'size': '250'}, {'price': '0.42', 'size': '228'}, {'price': '0.43', 'size': '225'}, {'price': '0.44', 'size': '363'}, {'price': '0.45', 'size': '200'}, {'price': '0.46', 'size': '189'}, {'price': '0.47', 'size': '467'}, {'price': '0.48', 'size': '196'}, {'price': '0.49', 'size': '569.77'}], 'asks': [{'price': '0.99', 'size': '100471.46'}, {'price': '0.98', 'size': '16922.2'}, {'price': '0.97', 'size': '11291.01'}, {'price': '0.96', 'size': '852.85'}, {'price': '0.95', 'size': '6934.13'}, {'price': '0.94', 'size': '516.01'}, {'price': '0.93', 'size': '501.01'}, {'price': '0.92', 'size': '501.01'}, {'price': '0.91', 'size': '501.01'}, {'price': '0.9', 'size': '3010'}, {'price': '0.89', 'size': '11.72'}, {'price': '0.85', 'size': '2178.75'}, {'price': '0.82', 'size': '5'}, {'price': '0.78', 'size': '10'}, {'price': '0.77', 'size': '1030'}, {'price': '0.76', 'size': '10'}, {'price': '0.75', 'size': '312.5'}, {'price': '0.74', 'size': '200'}, {'price': '0.73', 'size': '210'}, {'price': '0.72', 'size': '720'}, {'price': '0.71', 'size': '320'}, {'price': '0.7', 'size': '320'}, {'price': '0.67', 'size': '430'}, {'price': '0.65', 'size': '75'}, {'price': '0.64', 'size': '360'}, {'price': '0.62', 'size': '159'}, {'price': '0.61', 'size': '145'}, {'price': '0.6', 'size': '493'}, {'price': '0.59', 'size': '250'}, {'price': '0.58', 'size': '238'}, {'price': '0.57', 'size': '235'}, {'price': '0.56', 'size': '373'}, {'price': '0.55', 'size': '200'}, {'price': '0.54', 'size': '189'}, {'price': '0.53', 'size': '467'}, {'price': '0.52', 'size': '196'}, {'price': '0.51', 'size': '559.77'}, {'price': '0.5', 'size': '35'}], 'tick_size': '0.01', 'event_type': 'book', 'last_trade_price': '', 'exch_ts_sec': 1791549168, 'exch_ts_micro': 269999, 'sys_ts_sec': 1791549192, 'sys_ts_micro': 363428, 'channel': '12436205797886312193596951766498729222717998136132387193192101446751184826996'}, {'market': '0x706a9a416bab9d8bc65b780a1fdd5297cbfd78b1f8a6c91b888bb1b2b45d4646', 'asset_id': '61024487497240590399322467351097603162056598293957747867990027404577261185806', 'timestamp': '1791549160499', 'hash': '7083c6f5d0dbf95682b13fcd0a5fe2f008db729e', 'bids': [{'price': '0.01', 'size': '100302.33'}, {'price': '0.02', 'size': '16901.52'}, {'price': '0.03', 'size': '11331.01'}, {'price': '0.04', 'size': '836.01'}, {'price': '0.05', 'size': '6929.13'}, {'price': '0.06', 'size': '526.01'}, {'price': '0.07', 'size': '511.01'}, {'price': '0.08', 'size': '511.01'}, {'price': '0.09', 'size': '511.01'}, {'price': '0.1', 'size': '3010'}, {'price': '0.11', 'size': '10'}, {'price': '0.12', 'size': '10'}, {'price': '0.13', 'size': '10'}, {'price': '0.14', 'size': '10'}, {'price': '0.15', 'size': '2178.75'}, {'price': '0.16', 'size': '10'}, {'price': '0.18', 'size': '15'}, {'price': '0.2', 'size': '10'}, {'price': '0.21', 'size': '10'}, {'price': '0.22', 'size': '10'}, {'price': '0.23', 'size': '10'}, {'price': '0.24', 'size': '10'}, {'price': '0.25', 'size': '122.5'}, {'price': '0.26', 'size': '210'}, {'price': '0.27', 'size': '10'}, {'price': '0.28', 'size': '10'}, {'price': '0.29', 'size': '330'}, {'price': '0.3', 'size': '330'}, {'price': '0.31', 'size': '210'}, {'price': '0.32', 'size': '10'}, {'price': '0.33', 'size': '10'}, {'price': '0.34', 'size': '10'}, {'price': '0.35', 'size': '85'}, {'price': '0.37', 'size': '10'}, {'price': '0.38', 'size': '159'}, {'price': '0.39', 'size': '155'}, {'price': '0.4', 'size': '263'}, {'price': '0.41', 'size': '260'}, {'price': '0.42', 'size': '238'}, {'price': '0.43', 'size': '225'}, {'price': '0.44', 'size': '213'}, {'price': '0.45', 'size': '210'}, {'price': '0.46', 'size': '199'}, {'price': '0.47', 'size': '387'}, {'price': '0.48', 'size': '201'}, {'price': '0.49', 'size': '559.77'}, {'price': '0.5', 'size': '35'}], 'asks': [{'price': '0.99', 'size': '100275.85'}, {'price': '0.98', 'size': '16901.27'}, {'price': '0.97', 'size': '11331.01'}, {'price': '0.96', 'size': '786.01'}, {'price': '0.95', 'size': '6929.13'}, {'price': '0.94', 'size': '526.01'}, {'price': '0.93', 'size': '511.01'}, {'price': '0.92', 'size': '511.01'}, {'price': '0.91', 'size': '511.01'}, {'price': '0.9', 'size': '3010'}, {'price': '0.89', 'size': '10'}, {'price': '0.88', 'size': '10'}, {'price': '0.87', 'size': '10'}, {'price': '0.86', 'size': '10'}, {'price': '0.85', 'size': '2178.75'}, {'price': '0.84', 'size': '10'}, {'price': '0.82', 'size': '15'}, {'price': '0.8', 'size': '10'}, {'price': '0.79', 'size': '10'}, {'price': '0.78', 'size': '10'}, {'price': '0.77', 'size': '10'}, {'price': '0.76', 'size': '10'}, {'price': '0.75', 'size': '322.5'}, {'price': '0.74', 'size': '10'}, {'price': '0.73', 'size': '10'}, {'price': '0.72', 'size': '210'}, {'price': '0.71', 'size': '330'}, {'price': '0.7', 'size': '330'}, {'price': '0.69', 'size': '210'}, {'price': '0.68', 'size': '10'}, {'price': '0.67', 'size': '10'}, {'price': '0.66', 'size': '10'}, {'price': '0.65', 'size': '85'}, {'price': '0.63', 'size': '10'}, {'price': '0.62', 'size': '159'}, {'price': '0.61', 'size': '155'}, {'price': '0.6', 'size': '263'}, {'price': '0.59', 'size': '260'}, {'price': '0.58', 'size': '238'}, {'price': '0.57', 'size': '225'}, {'price': '0.56', 'size': '213'}, {'price': '0.55', 'size': '210'}, {'price': '0.54', 'size': '199'}, {'price': '0.53', 'size': '387'}, {'price': '0.52', 'size': '201'}, {'price': '0.51', 'size': '569.77'}], 'tick_size': '0.01', 'event_type': 'book', 'last_trade_price': '', 'exch_ts_sec': 1791549160, 'exch_ts_micro': 499000, 'sys_ts_sec': 1791549192, 'sys_ts_micro': 363428, 'channel': '61024487497240590399322467351097603162056598293957747867990027404577261185806'}, {'market': '0x706a9a416bab9d8bc65b780a1fdd5297cbfd78b1f8a6c91b888bb1b2b45d4646', 'asset_id': '80787442587856751219701037219674284427871157182176559743196374119442676346453', 'timestamp': '1791549160499', 'hash': '1a74f83e0d070d483a18d3fb4b569279e7437925', 'bids': [{'price': '0.01', 'size': '100275.85'}, {'price': '0.02', 'size': '16901.27'}, {'price': '0.03', 'size': '11331.01'}, {'price': '0.04', 'size': '786.01'}, {'price': '0.05', 'size': '6929.13'}, {'price': '0.06', 'size': '526.01'}, {'price': '0.07', 'size': '511.01'}, {'price': '0.08', 'size': '511.01'}, {'price': '0.09', 'size': '511.01'}, {'price': '0.1', 'size': '3010'}, {'price': '0.11', 'size': '10'}, {'price': '0.12', 'size': '10'}, {'price': '0.13', 'size': '10'}, {'price': '0.14', 'size': '10'}, {'price': '0.15', 'size': '2178.75'}, {'price': '0.16', 'size': '10'}, {'price': '0.18', 'size': '15'}, {'price': '0.2', 'size': '10'}, {'price': '0.21', 'size': '10'}, {'price': '0.22', 'size': '10'}, {'price': '0.23', 'size': '10'}, {'price': '0.24', 'size': '10'}, {'price': '0.25', 'size': '322.5'}, {'price': '0.26', 'size': '10'}, {'price': '0.27', 'size': '10'}, {'price': '0.28', 'size': '210'}, {'price': '0.29', 'size': '330'}, {'price': '0.3', 'size': '330'}, {'price': '0.31', 'size': '210'}, {'price': '0.32', 'size': '10'}, {'price': '0.33', 'size': '10'}, {'price': '0.34', 'size': '10'}, {'price': '0.35', 'size': '85'}, {'price': '0.37', 'size': '10'}, {'price': '0.38', 'size': '159'}, {'price': '0.39', 'size': '155'}, {'price': '0.4', 'size': '263'}, {'price': '0.41', 'size': '260'}, {'price': '0.42', 'size': '238'}, {'price': '0.43', 'size': '225'}, {'price': '0.44', 'size': '213'}, {'price': '0.45', 'size': '210'}, {'price': '0.46', 'size': '199'}, {'price': '0.47', 'size': '387'}, {'price': '0.48', 'size': '201'}, {'price': '0.49', 'size': '569.77'}], 'asks': [{'price': '0.99', 'size': '100302.33'}, {'price': '0.98', 'size': '16901.52'}, {'price': '0.97', 'size': '11331.01'}, {'price': '0.96', 'size': '836.01'}, {'price': '0.95', 'size': '6929.13'}, {'price': '0.94', 'size': '526.01'}, {'price': '0.93', 'size': '511.01'}, {'price': '0.92', 'size': '511.01'}, {'price': '0.91', 'size': '511.01'}, {'price': '0.9', 'size': '3010'}, {'price': '0.89', 'size': '10'}, {'price': '0.88', 'size': '10'}, {'price': '0.87', 'size': '10'}, {'price': '0.86', 'size': '10'}, {'price': '0.85', 'size': '2178.75'}, {'price': '0.84', 'size': '10'}, {'price': '0.82', 'size': '15'}, {'price': '0.8', 'size': '10'}, {'price': '0.79', 'size': '10'}, {'price': '0.78', 'size': '10'}, {'price': '0.77', 'size': '10'}, {'price': '0.76', 'size': '10'}, {'price': '0.75', 'size': '122.5'}, {'price': '0.74', 'size': '210'}, {'price': '0.73', 'size': '10'}, {'price': '0.72', 'size': '10'}, {'price': '0.71', 'size': '330'}, {'price': '0.7', 'size': '330'}, {'price': '0.69', 'size': '210'}, {'price': '0.68', 'size': '10'}, {'price': '0.67', 'size': '10'}, {'price': '0.66', 'size': '10'}, {'price': '0.65', 'size': '85'}, {'price': '0.63', 'size': '10'}, {'price': '0.62', 'size': '159'}, {'price': '0.61', 'size': '155'}, {'price': '0.6', 'size': '263'}, {'price': '0.59', 'size': '260'}, {'price': '0.58', 'size': '238'}, {'price': '0.57', 'size': '225'}, {'price': '0.56', 'size': '213'}, {'price': '0.55', 'size': '210'}, {'price': '0.54', 'size': '199'}, {'price': '0.53', 'size': '387'}, {'price': '0.52', 'size': '201'}, {'price': '0.51', 'size': '559.77'}, {'price': '0.5', 'size': '35'}], 'tick_size': '0.01', 'event_type': 'book', 'last_trade_price': '', 'exch_ts_sec': 1791549160, 'exch_ts_micro': 499000, 'sys_ts_sec': 1791549192, 'sys_ts_micro': 363428, 'channel': '80787442587856751219701037219674284427871157182176559743196374119442676346453'}, {'market': '0x12ceccda15598549aa70da45ed0ff62e5d657cb59e5ac907a6e008c5dbb8a8fa', 'asset_id': '73432345403558778216509911841506585248627348303948893840311045405335993091482', 'timestamp': '1791549160071', 'hash': '4242a6dbbe44c827abd170df2834d0a412ed7928', 'bids': [{'price': '0.01', 'size': '99973.53'}, {'price': '0.02', 'size': '16971.63'}, {'price': '0.03', 'size': '11316.01'}, {'price': '0.04', 'size': '856.01'}, {'price': '0.05', 'size': '6934.13'}, {'price': '0.06', 'size': '516.01'}, {'price': '0.07', 'size': '501.01'}, {'price': '0.08', 'size': '501.01'}, {'price': '0.09', 'size': '501.01'}, {'price': '0.1', 'size': '3010'}, {'price': '0.11', 'size': '10'}, {'price': '0.12', 'size': '10'}, {'price': '0.13', 'size': '10'}, {'price': '0.15', 'size': '2178.75'}, {'price': '0.17', 'size': '10'}, {'price': '0.18', 'size': '10'}, {'price': '0.2', 'size': '10'}, {'price': '0.21', 'size': '10'}, {'price': '0.22', 'size': '10'}, {'price': '0.25', 'size': '322.5'}, {'price': '0.26', 'size': '10'}, {'price': '0.27', 'size': '10'}, {'price': '0.28', 'size': '200'}, {'price': '0.29', 'size': '330'}, {'price': '0.3', 'size': '330'}, {'price': '0.31', 'size': '210'}, {'price': '0.33', 'size': '10'}, {'price': '0.34', 'size': '10'}, {'price': '0.35', 'size': '75'}, {'price': '0.37', 'size': '10'}, {'price': '0.38', 'size': '149'}, {'price': '0.39', 'size': '155'}, {'price': '0.4', 'size': '253'}, {'price': '0.41', 'size': '260'}, {'price': '0.42', 'size': '238'}, {'price': '0.43', 'size': '225'}, {'price': '0.44', 'size': '213'}, {'price': '0.45', 'size': '210'}, {'price': '0.46', 'size': '189'}, {'price': '0.47', 'size': '387'}, {'price': '0.48', 'size': '196'}, {'price': '0.49', 'size': '560.19'}, {'price': '0.5', 'size': '30'}], 'asks': [{'price': '0.99', 'size': '99946.81'}, {'price': '0.98', 'size': '16971.1'}, {'price': '0.97', 'size': '11316.01'}, {'price': '0.96', 'size': '806.01'}, {'price': '0.95', 'size': '6934.13'}, {'price': '0.94', 'size': '516.01'}, {'price': '0.93', 'size': '501.01'}, {'price': '0.92', 'size': '501.01'}, {'price': '0.91', 'size': '501.01'}, {'price': '0.9', 'size': '3010'}, {'price': '0.89', 'size': '10'}, {'price': '0.88', 'size': '10'}, {'price': '0.87', 'size': '10'}, {'price': '0.85', 'size': '2178.75'}, {'price': '0.83', 'size': '10'}, {'price': '0.82', 'size': '10'}, {'price': '0.8', 'size': '10'}, {'price': '0.79', 'size': '10'}, {'price': '0.78', 'size': '10'}, {'price': '0.75', 'size': '322.5'}, {'price': '0.74', 'size': '210'}, {'price': '0.73', 'size': '210'}, {'price': '0.71', 'size': '330'}, {'price': '0.7', 'size': '330'}, {'price': '0.69', 'size': '10'}, {'price': '0.67', 'size': '10'}, {'price': '0.66', 'size': '10'}, {'price': '0.65', 'size': '75'}, {'price': '0.63', 'size': '10'}, {'price': '0.62', 'size': '149'}, {'price': '0.61', 'size': '155'}, {'price': '0.6', 'size': '253'}, {'price': '0.59', 'size': '260'}, {'price': '0.58', 'size': '238'}, {'price': '0.57', 'size': '225'}, {'price': '0.56', 'size': '213'}, {'price': '0.55', 'size': '210'}, {'price': '0.54', 'size': '189'}, {'price': '0.53', 'size': '387'}, {'price': '0.52', 'size': '196'}, {'price': '0.51', 'size': '565.19'}], 'tick_size': '0.01', 'event_type': 'book', 'last_trade_price': '', 'exch_ts_sec': 1791549160, 'exch_ts_micro': 71000, 'sys_ts_sec': 1791549192, 'sys_ts_micro': 363428, 'channel': '73432345403558778216509911841506585248627348303948893840311045405335993091482'}, {'market': '0x12ceccda15598549aa70da45ed0ff62e5d657cb59e5ac907a6e008c5dbb8a8fa', 'asset_id': '9510719965850420024799447271914145758557425424360732340799488076869718606039', 'timestamp': '1791549160071', 'hash': 'f0b337fc019bd32a6555f181022a8629b11fce55', 'bids': [{'price': '0.01', 'size': '99946.81'}, {'price': '0.02', 'size': '16971.1'}, {'price': '0.03', 'size': '11316.01'}, {'price': '0.04', 'size': '806.01'}, {'price': '0.05', 'size': '6934.13'}, {'price': '0.06', 'size': '516.01'}, {'price': '0.07', 'size': '501.01'}, {'price': '0.08', 'size': '501.01'}, {'price': '0.09', 'size': '501.01'}, {'price': '0.1', 'size': '3010'}, {'price': '0.11', 'size': '10'}, {'price': '0.12', 'size': '10'}, {'price': '0.13', 'size': '10'}, {'price': '0.15', 'size': '2178.75'}, {'price': '0.17', 'size': '10'}, {'price': '0.18', 'size': '10'}, {'price': '0.2', 'size': '10'}, {'price': '0.21', 'size': '10'}, {'price': '0.22', 'size': '10'}, {'price': '0.25', 'size': '322.5'}, {'price': '0.26', 'size': '210'}, {'price': '0.27', 'size': '210'}, {'price': '0.29', 'size': '330'}, {'price': '0.3', 'size': '330'}, {'price': '0.31', 'size': '10'}, {'price': '0.33', 'size': '10'}, {'price': '0.34', 'size': '10'}, {'price': '0.35', 'size': '75'}, {'price': '0.37', 'size': '10'}, {'price': '0.38', 'size': '149'}, {'price': '0.39', 'size': '155'}, {'price': '0.4', 'size': '253'}, {'price': '0.41', 'size': '260'}, {'price': '0.42', 'size': '238'}, {'price': '0.43', 'size': '225'}, {'price': '0.44', 'size': '213'}, {'price': '0.45', 'size': '210'}, {'price': '0.46', 'size': '189'}, {'price': '0.47', 'size': '387'}, {'price': '0.48', 'size': '196'}, {'price': '0.49', 'size': '565.19'}], 'asks': [{'price': '0.99', 'size': '99973.53'}, {'price': '0.98', 'size': '16971.63'}, {'price': '0.97', 'size': '11316.01'}, {'price': '0.96', 'size': '856.01'}, {'price': '0.95', 'size': '6934.13'}, {'price': '0.94', 'size': '516.01'}, {'price': '0.93', 'size': '501.01'}, {'price': '0.92', 'size': '501.01'}, {'price': '0.91', 'size': '501.01'}, {'price': '0.9', 'size': '3010'}, {'price': '0.89', 'size': '10'}, {'price': '0.88', 'size': '10'}, {'price': '0.87', 'size': '10'}, {'price': '0.85', 'size': '2178.75'}, {'price': '0.83', 'size': '10'}, {'price': '0.82', 'size': '10'}, {'price': '0.8', 'size': '10'}, {'price': '0.79', 'size': '10'}, {'price': '0.78', 'size': '10'}, {'price': '0.75', 'size': '322.5'}, {'price': '0.74', 'size': '10'}, {'price': '0.73', 'size': '10'}, {'price': '0.72', 'size': '200'}, {'price': '0.71', 'size': '330'}, {'price': '0.7', 'size': '330'}, {'price': '0.69', 'size': '210'}, {'price': '0.67', 'size': '10'}, {'price': '0.66', 'size': '10'}, {'price': '0.65', 'size': '75'}, {'price': '0.63', 'size': '10'}, {'price': '0.62', 'size': '149'}, {'price': '0.61', 'size': '155'}, {'price': '0.6', 'size': '253'}, {'price': '0.59', 'size': '260'}, {'price': '0.58', 'size': '238'}, {'price': '0.57', 'size': '225'}, {'price': '0.56', 'size': '213'}, {'price': '0.55', 'size': '210'}, {'price': '0.54', 'size': '189'}, {'price': '0.53', 'size': '387'}, {'price': '0.52', 'size': '196'}, {'price': '0.51', 'size': '560.19'}, {'price': '0.5', 'size': '30'}], 'tick_size': '0.01', 'event_type': 'book', 'last_trade_price': '', 'exch_ts_sec': 1791549160, 'exch_ts_micro': 71000, 'sys_ts_sec': 1791549192, 'sys_ts_micro': 363428, 'channel': '9510719965850420024799447271914145758557425424360732340799488076869718606039'}]}

    assert ideal_data == data


@pytest.mark.asyncio
async def test_model_data_book_secondary_validate(polymarket_adapter, model_data_book_secondary):
    data = polymarket_adapter.valid_message_can_pass_and_restructure_data(msg=model_data_book_secondary)

    assert "data" in data
    assert "valid" in data
    assert data["valid"]
    assert isinstance(data["data"], list)
    assert "channel" in data["data"][0]

    ideal_data = {'valid': True, 'data': [{'market': '0x20c2c0c2d897d1bf2116f258ca7104fe9e5e3ae97d9c4ea2b0f4f70078a51544', 'asset_id': '1316211778311329685045645979366681620182409688329148910713383446531301807652', 'bids': [{'price': '0.01', 'size': '8489.25'}, {'price': '0.02', 'size': '3028.46'}, {'price': '0.03', 'size': '5047.37'}, {'price': '0.04', 'size': '1043.08'}, {'price': '0.05', 'size': '1003.55'}, {'price': '0.06', 'size': '1025.88'}, {'price': '0.07', 'size': '796.43'}, {'price': '0.08', 'size': '683.51'}, {'price': '0.09', 'size': '754.56'}, {'price': '0.1', 'size': '181'}, {'price': '0.11', 'size': '224'}, {'price': '0.12', 'size': '276.32'}, {'price': '0.13', 'size': '140'}, {'price': '0.14', 'size': '68'}, {'price': '0.15', 'size': '275.98'}, {'price': '0.16', 'size': '262'}, {'price': '0.17', 'size': '498.01'}, {'price': '0.18', 'size': '158'}, {'price': '0.19', 'size': '142.31'}, {'price': '0.2', 'size': '293'}, {'price': '0.21', 'size': '150'}, {'price': '0.22', 'size': '138'}, {'price': '0.23', 'size': '143'}, {'price': '0.24', 'size': '136'}, {'price': '0.25', 'size': '169'}, {'price': '0.26', 'size': '143'}, {'price': '0.27', 'size': '132'}, {'price': '0.28', 'size': '105'}, {'price': '0.29', 'size': '1634'}, {'price': '0.3', 'size': '121.36'}, {'price': '0.31', 'size': '59.33'}, {'price': '0.32', 'size': '30'}, {'price': '0.33', 'size': '30'}, {'price': '0.34', 'size': '65'}, {'price': '0.35', 'size': '72.24'}], 'asks': [{'price': '0.99', 'size': '8579.64'}, {'price': '0.98', 'size': '2973.46'}, {'price': '0.97', 'size': '3620.43'}, {'price': '0.96', 'size': '8428.08'}, {'price': '0.95', 'size': '7128.55'}, {'price': '0.94', 'size': '1390.88'}, {'price': '0.93', 'size': '2790'}, {'price': '0.92', 'size': '1298.51'}, {'price': '0.91', 'size': '1329.56'}, {'price': '0.9', 'size': '658'}, {'price': '0.89', 'size': '662.53'}, {'price': '0.88', 'size': '89'}, {'price': '0.87', 'size': '1647'}, {'price': '0.86', 'size': '73'}, {'price': '0.85', 'size': '132'}, {'price': '0.84', 'size': '269'}, {'price': '0.83', 'size': '73'}, {'price': '0.82', 'size': '65'}, {'price': '0.81', 'size': '75'}, {'price': '0.8', 'size': '1222.9'}, {'price': '0.79', 'size': '173'}, {'price': '0.78', 'size': '60'}, {'price': '0.77', 'size': '192'}, {'price': '0.76', 'size': '59'}, {'price': '0.75', 'size': '74'}, {'price': '0.74', 'size': '157'}, {'price': '0.73', 'size': '67'}, {'price': '0.72', 'size': '56'}, {'price': '0.71', 'size': '41'}, {'price': '0.7', 'size': '733'}, {'price': '0.69', 'size': '156'}, {'price': '0.68', 'size': '64'}, {'price': '0.67', 'size': '56'}, {'price': '0.66', 'size': '88'}, {'price': '0.65', 'size': '101'}, {'price': '0.64', 'size': '177'}, {'price': '0.63', 'size': '66'}, {'price': '0.62', 'size': '57'}, {'price': '0.61', 'size': '46'}, {'price': '0.6', 'size': '343.99'}, {'price': '0.59', 'size': '157'}, {'price': '0.58', 'size': '228'}, {'price': '0.57', 'size': '99'}, {'price': '0.56', 'size': '301'}, {'price': '0.55', 'size': '386'}, {'price': '0.54', 'size': '295'}, {'price': '0.53', 'size': '294'}, {'price': '0.52', 'size': '151'}, {'price': '0.51', 'size': '168.01'}, {'price': '0.5', 'size': '175'}, {'price': '0.49', 'size': '298'}, {'price': '0.48', 'size': '299'}, {'price': '0.47', 'size': '309'}, {'price': '0.46', 'size': '335'}, {'price': '0.45', 'size': '411'}, {'price': '0.44', 'size': '110'}, {'price': '0.43', 'size': '160'}, {'price': '0.42', 'size': '199'}, {'price': '0.41', 'size': '241'}, {'price': '0.4', 'size': '636'}, {'price': '0.39', 'size': '1105'}, {'price': '0.38', 'size': '480.62'}, {'price': '0.37', 'size': '333.7'}, {'price': '0.36', 'size': '1605.61'}], 'hash': 'f5b4ea10031d1d53d17985e147352f21e59f0470', 'timestamp': '1791627916999', 'event_type': 'book', 'sys_time': 1791627916.902125, 'exch_ts_sec': 1791627916, 'exch_ts_micro': 999000, 'sys_ts_sec': 1791627916, 'sys_ts_micro': 902124, 'channel': '1316211778311329685045645979366681620182409688329148910713383446531301807652'}]}

    assert ideal_data == data


@pytest.mark.asyncio
async def test_model_data_price_changes_validate(polymarket_adapter, model_data_price_changes):
    data = polymarket_adapter.valid_message_can_pass_and_restructure_data(msg=model_data_price_changes)

    assert "data" in data
    assert "valid" in data
    assert data["valid"]
    assert isinstance(data["data"], list)
    assert "channel" in data["data"][0]

    ideal_data = {'valid': True, 'data': [{'asset_id': '92365246247895914650049242136497506323541732213680451262412888117420267760503', 'price': '0.49', 'size': '117', 'side': 'BUY', 'hash': '4344e1e80ebb6ebe95a8fb63971fa31d79a87b0e', 'best_bid': '0.49', 'best_ask': '1', 'market': '0xd37a02d8e4f5340577add930531b51bbac184eacfff13132cb869ed663194ae4', 'timestamp': '1791549207492', 'event_type': 'price_change', 'sys_time': 1791549381.2840257, 'exch_ts_sec': 1791549207, 'exch_ts_micro': 492000, 'sys_ts_sec': 1791549381, 'sys_ts_micro': 284025, 'channel': '92365246247895914650049242136497506323541732213680451262412888117420267760503'}, {'asset_id': '24732826355128432486815107973143652972809090770361689526737953099368890068252', 'price': '0.51', 'size': '117', 'side': 'SELL', 'hash': 'a41adae811e4c58407c22c5c19e19b5f50c5b18a', 'best_bid': '0', 'best_ask': '0.51', 'market': '0xd37a02d8e4f5340577add930531b51bbac184eacfff13132cb869ed663194ae4', 'timestamp': '1791549207492', 'event_type': 'price_change', 'sys_time': 1791549381.2840257, 'exch_ts_sec': 1791549207, 'exch_ts_micro': 492000, 'sys_ts_sec': 1791549381, 'sys_ts_micro': 284025, 'channel': '24732826355128432486815107973143652972809090770361689526737953099368890068252'}]}

    assert ideal_data == data


@pytest.mark.asyncio
async def test_model_data_tick_size_validate(polymarket_adapter, model_data_tick_size_changes):
    data = polymarket_adapter.valid_message_can_pass_and_restructure_data(msg=model_data_tick_size_changes)

    assert "data" in data
    assert "valid" in data
    assert data["valid"]
    assert isinstance(data["data"], list)
    assert "channel" in data["data"][0]

    ideal_data = {'valid': True, 'data': [{'market': '0xfac1c6c31fc896220699b8f2ccad0459f690f5b98a7d6f7cfcac999e7bfc48ed', 'asset_id': '42215505728357508195209790760712417278627646406240537861497755485097801777900', 'old_tick_size': '0.01', 'new_tick_size': '0.001', 'timestamp': '1791627334385', 'event_type': 'tick_size_change', 'sys_time': 1791627334.2894661, 'exch_ts_sec': 1791627334, 'exch_ts_micro': 384999, 'sys_ts_sec': 1791627334, 'sys_ts_micro': 289466, 'channel': '42215505728357508195209790760712417278627646406240537861497755485097801777900'}]}

    assert ideal_data == data


@pytest.mark.asyncio
async def test_model_data_last_trade_price_validate(polymarket_adapter, model_data_last_trade_price):
    data = polymarket_adapter.valid_message_can_pass_and_restructure_data(msg=model_data_last_trade_price)

    assert "data" in data
    assert "valid" in data
    assert data["valid"]
    assert isinstance(data["data"], list)
    assert "channel" in data["data"][0]

    ideal_data = {'valid': True, 'data': [
        {'market': '0x1fd1459480c91ba91040665fe905f9b913e12d49cb0c345f72f7a7b0dd7d4e23',
         'asset_id': '31403232306013778162476061765940015949835322748651947415662658615459066883162', 'price': '0.74',
         'size': '1.35135', 'fee_rate_bps': '0', 'side': 'BUY', 'timestamp': '1791627513118',
         'event_type': 'last_trade_price',
         'transaction_hash': '0xf0c5f60115b5837c38756842f653af811201abc760e1a7064fd53408702f0391',
         'sys_time': 1791627709.034008, 'exch_ts_sec': 1791627513, 'exch_ts_micro': 118000, 'sys_ts_sec': 1791627709,
         'sys_ts_micro': 34008,
         'channel': '31403232306013778162476061765940015949835322748651947415662658615459066883162'}]}

    assert ideal_data == data






@pytest.mark.asyncio
async def test_channels_and_pipelines_removal_and_addition(polymarket_adapter_config):
    pipelines = []
    limit_number_of_channels = 2
    total_number_of_channels = 8
    additional_channels = 3
    polymarket_manager = PolymarketManager(pipelines=pipelines, limit_of_number_of_channels=limit_number_of_channels)

    list_of_instruments = polymarket_manager.find_instruments(config=polymarket_adapter_config)

    dict_of_channels = polymarket_manager.format_instruments_to_channels(list_of_instruments, polymarket_adapter_config)

    keys = list(dict_of_channels.keys())[-additional_channels:]
    remaining_keys = list(dict_of_channels.keys())[:total_number_of_channels]

    new_dict_channels = {k: dict_of_channels[k] for k in keys}

    dict_of_channels = {k: dict_of_channels[k] for k in remaining_keys}

    # adding new channels from scratch
    await polymarket_manager.add_channels(channels_to_acquire=dict_of_channels, config=polymarket_adapter_config)

    dict_of_channels_to_pipeline = polymarket_manager.decompile_channels_to_pipeline()
    polymarket_manager.set_dict_of_channels_to_pipeline(dict_of_channels_to_pipeline=dict_of_channels_to_pipeline)

    assert len(pipelines) == (total_number_of_channels / limit_number_of_channels)
    assert set(dict_of_channels.keys()) == set(dict_of_channels_to_pipeline.keys())

    await asyncio.sleep(60)

    # removing every other channel so every pipeline has a channels removed from it
    removal_list = list(dict_of_channels.keys())[::2]
    channels_to_remove = {k: dict_of_channels[k] for k in removal_list}

    await polymarket_manager.remove_channels(channels_to_remove=channels_to_remove)

    dict_of_channels_to_pipeline = polymarket_manager.decompile_channels_to_pipeline()
    polymarket_manager.set_dict_of_channels_to_pipeline(dict_of_channels_to_pipeline=dict_of_channels_to_pipeline)

    assert len(pipelines) == (total_number_of_channels / limit_number_of_channels)
    remaining_set_of_channels: set = dict_of_channels.keys() - channels_to_remove.keys()
    assert remaining_set_of_channels == set(dict_of_channels_to_pipeline.keys())

    temp = {}
    temp.update(channels_to_remove)
    temp.update(new_dict_channels)
    # adding new channels
    await polymarket_manager.add_channels(channels_to_acquire=temp, config=polymarket_adapter_config)

    dict_of_channels_to_pipeline = polymarket_manager.decompile_channels_to_pipeline()
    polymarket_manager.set_dict_of_channels_to_pipeline(dict_of_channels_to_pipeline=dict_of_channels_to_pipeline)

    assert (additional_channels + total_number_of_channels) == len(dict_of_channels_to_pipeline.keys())
    assert len(pipelines) == math.ceil((additional_channels + total_number_of_channels) / limit_number_of_channels)

    await asyncio.sleep(60)

    # removing new channels
    await polymarket_manager.remove_channels(channels_to_remove=new_dict_channels)

    dict_of_channels_to_pipeline = polymarket_manager.decompile_channels_to_pipeline()
    polymarket_manager.set_dict_of_channels_to_pipeline(dict_of_channels_to_pipeline=dict_of_channels_to_pipeline)

    assert len(pipelines) == math.ceil(total_number_of_channels / limit_number_of_channels)
    assert len(dict_of_channels_to_pipeline.keys()) == total_number_of_channels

    removal_dict_whole = {key: PolymarketAdapter.event_info.get(key) for key in dict_of_channels_to_pipeline.keys()}
    # removing all channels and see how all the pipelines will close
    await polymarket_manager.remove_channels(channels_to_remove=removal_dict_whole)

    assert len(pipelines) == 0


def test_rest_request_to_exchange_to_get_instruments(polymarket_adapter_config):
    information = PolymarketAdapter.get_instruments(polymarket_adapter_config)
    # function below does similar logic and there is a ratelimit on how many times you can call the function above, thus sleep gives a break.
    time.sleep(1)

    assert (isinstance(information, list)
            and len(information) > 0
            and "id" in information[0]
            and "markets" in information[0]
            and "ticker" in information[0]
            and isinstance(information[0]["markets"], list)
            and "outcomes" in information[0]["markets"][0]
            and "clobTokenIds" in information[0]["markets"][0]
            and isinstance(information[0]["markets"][0]["outcomes"], str)
            and isinstance(information[0]["markets"][0]["clobTokenIds"], str)
            )


def test_helper_function_to_make_multiple_adapters(polymarket_adapter_config):
    polymarket_adapters = PolymarketManager.generate_multiple_adapters(polymarket_adapter_config)

    channel_number_under_limit = True
    list_of_channels = []
    for _polymarket_adapter in polymarket_adapters:

        channel_number_under_limit &= len(
            _polymarket_adapter.msg['assets_ids']) <= polymarket_adapter_config.limit_number_of_channels

        list_of_channels += copy.copy(_polymarket_adapter.msg['assets_ids'])

    assert len(list_of_channels) > 0
    # no duplicates all channels have unique values
    assert len(list_of_channels) == len(set(list_of_channels))
    assert channel_number_under_limit

def read_file(path_to_file):
    event = None
    try:
        with gzip.open(path_to_file, "rt", encoding="utf-8") as f:
            event = json.loads(f.read().replace(r"\n", ""))

    except Exception as e:
        print(e)

    return event

def test_saving_data_from_polymarket_file_writer(polymarket_adapter, model_data_book_initial, provide_event_info):
    data = model_data_book_initial
    event_info = provide_event_info
    key = list(event_info.keys())[0]

    PolymarketAdapter.add_dict_to_event_info_dict(event_info)

    start = time.time()
    polymarket_adapter.writer(data)
    end = time.time() - start
    print("time taken to save data: ",end)

    PATH_DIR = Path("data")
    dirs = PATH_DIR / polymarket_adapter.get_exchange_name()
    files = [path for path in dirs.rglob("*") if path.is_file()]

    flag1 = False
    flag2 = False

    for file in files:
        if "meta_data.json" in str(file):
            flag1 = True
            meta_data = read_file(file)
            test = json.loads( (json.dumps(event_info[key])).replace(r"\n", "") )
            assert test == meta_data

        if "jsonl" in str(file):
            flag2 = True
            event_data = read_file(file)
            assert data == event_data

    assert flag1 and flag2

    if dirs.exists() and dirs.is_dir():
        shutil.rmtree(dirs)



def test_compression_and_time_taken(polymarket_adapter, model_data_book_initial, provide_event_info):
    data = model_data_book_initial
    event_info = provide_event_info
    key = list(event_info.keys())[0]

    PolymarketAdapter.add_dict_to_event_info_dict(event_info)

    polymarket_adapter.writer(data)

    PATH_DIR = Path("data")
    dirs = PATH_DIR / polymarket_adapter.get_exchange_name()
    files = [path for path in dirs.rglob("*") if path.is_file()]

    flag1 = False
    flag2 = False

    for file in files:
        if "meta_data.json" in str(file):
            flag1 = True
            meta_data = read_file(file)
            test = json.loads( (json.dumps(event_info[key])).replace(r"\n", "") )
            assert test == meta_data

        if "jsonl" in str(file):
            flag2 = True
            event_data = read_file(file)
            assert data == event_data

    assert flag1 and flag2

    if dirs.exists() and dirs.is_dir():
        shutil.rmtree(dirs)




