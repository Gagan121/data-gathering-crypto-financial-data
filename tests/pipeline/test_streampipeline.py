import asyncio

import pytest
from core.exchanges.exchange_adapter import ExchangeAdapter
from core.pipeline.streampipeline import StreamPipeline


@pytest.fixture
def adapter() -> ExchangeAdapter:
    class TestAdapter(ExchangeAdapter):
        def validate_message(self, msg):
            return True
        def normalise_data(self, batch_list:list) -> list:
            return batch_list
        def writer(self, normalised_list_of_data):
            return
    path_to_folder = r"D:\python_projects\data_gathering_using_websockets_finance_crypto\data"
    return TestAdapter(path_to_folder=path_to_folder, exchange_name="Test", url="ws:test", msg={})

@pytest.fixture
def pipeline(adapter) -> StreamPipeline:
    return StreamPipeline(exchange_adapter=adapter)


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

@pytest.mark.asyncio
async def test_process_batch_and_clears_batch(pipeline):
    pipeline.batch_list = [
        {"event": 0},
        {"event": 1},
    ]
    await pipeline.process_batch()

    assert len(pipeline.batch_list) == 0


@pytest.mark.asyncio
async def test_process_restore_batch_on_failure_on_normalisation_of_data(pipeline):
    previous_batch = [
        {"event": 0},
        {"event": 1},
    ]

    pipeline.batch_list = previous_batch
    def fail_normalisation(batch_list):
        raise Exception("bad data")

    pipeline.exchange_adapter.normalise_data = fail_normalisation

    await pipeline.process_batch()

    # gets all values of the old batch and checks if they are in the pipeline.batch_list to see if there has been a recovery -> all means every value must be true
    assert pipeline.batch_list == [{"event": 0},{"event": 1}]

def test_consumer_triggers_batch_process_and_write():
    pass

def test_consumer_empties_queue_on_cancel():
    pass

def test_consumer_pushes_remaining_batch_on_cancel():
    pass

@pytest.mark.asyncio
async def test_producer_pushes_valid_items_into_queue(pipeline):

    async def fake_stream():
        yield {"event":1}
        yield {"event":2}
    # hot swapping out the functions
    pipeline.ws.stream = fake_stream


    # ---------------------------------------------------------
    producer_task = asyncio.create_task(pipeline.producer())
    await asyncio.sleep(0.1)

    producer_task.cancel()

    assert pipeline.queue.qsize() == 2





def test_producer_refuses_new_messages_once_closed():
    pass

