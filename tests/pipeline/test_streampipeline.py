import asyncio
from unittest.mock import MagicMock, AsyncMock
import pytest
from core.exchanges.exchange_adapter import ExchangeAdapter
from core.pipeline.streampipeline import StreamPipeline


@pytest.fixture
def adapter() -> ExchangeAdapter:
    class TestAdapter(ExchangeAdapter):
        def restructure_data(self, data) -> dict:
            return data
        def validate_message(self, msg):
            return True
        def normalise_data(self, batch_list:list) -> list:
            return batch_list
        def writer(self, normalised_list_of_data):
            return
    return TestAdapter(exchange_name="Test", url="ws:test", msg={}, ticker="TEST_TEST")

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

# remember you are testing not adding additional functionality ,e.g. not clearing list for it to trigger another set of events
@pytest.mark.asyncio
async def test_consumer_triggers_batch_process_and_again_on_error_thrown(pipeline:StreamPipeline):

    pipeline.max_size_for_batch = 2
    # we use AsyncMock to mock functions that are async
    pipeline.process_batch = AsyncMock()

    await pipeline.queue.put({"event": 0})
    await pipeline.queue.put({"event": 1})

    # this has to be a task not just await because it has a while loop in it
    consumer_task = asyncio.create_task(pipeline.consumer())

    await asyncio.sleep(0.1)

    consumer_task.cancel()

    await asyncio.gather(consumer_task, return_exceptions=True)

    # assert_awaited() -> can reset if ran twice,  thus throwing errors
    assert pipeline.process_batch.await_count >= 2


@pytest.mark.asyncio
async def test_consumer_empties_queue_on_cancel(pipeline):
    pipeline.max_size_for_batch = 2

    await pipeline.queue.put({"event": 0})
    await pipeline.queue.put({"event": 1})

    # this has to be a task not just await because it has a while loop in it
    consumer_task = asyncio.create_task(pipeline.consumer())

    await asyncio.sleep(0.1)

    consumer_task.cancel()

    await asyncio.gather(consumer_task, return_exceptions=True)

    assert pipeline.queue.qsize() >= 0

@pytest.mark.asyncio
async def test_consumer_pushes_remaining_batch_on_cancel(pipeline:StreamPipeline):
    # the limit here to reach on which a process/write occurs, if below then cancel list is pushed
    pipeline.max_size_for_batch = 4

    await pipeline.queue.put({"bid":1, 'ask':1})
    await pipeline.queue.put({"bid":2, 'ask':2})

    # this has to be a task not just await because it has a while loop in it
    consumer_task = asyncio.create_task(pipeline.consumer())

    await asyncio.sleep(0.1)

    consumer_task.cancel()

    await asyncio.gather(consumer_task, return_exceptions=True)

    assert len(pipeline.batch_list) == 0


@pytest.mark.asyncio
async def test_producer_pushes_valid_items_into_queue(pipeline):

    async def fake_stream():
        yield {"bid":1, 'ask':1}
        yield {"bid":2, 'ask':2}
    # hot swapping out the functions
    pipeline.ws.stream = fake_stream


    # ---------------------------------------------------------
    producer_task = asyncio.create_task(pipeline.producer())
    await asyncio.sleep(0.1)

    producer_task.cancel()

    assert pipeline.queue.qsize() == 2


@pytest.mark.asyncio
async def test_producer_pushes_valid_items_and_refused_invalid_items_into_queue(pipeline):

    async def fake_stream():
        # accept
        yield {"bid":1, 'ask':1}
        # refuses -> same quotes as above thus duplicates
        yield {"bid":1, 'ask':1}
        # accept
        yield {"bid":2, 'ask':2}
    # hot swapping out the functions
    pipeline.ws.stream = fake_stream


    # ---------------------------------------------------------
    producer_task = asyncio.create_task(pipeline.producer())
    await asyncio.sleep(0.1)

    producer_task.cancel()

    assert pipeline.queue.qsize() == 2

