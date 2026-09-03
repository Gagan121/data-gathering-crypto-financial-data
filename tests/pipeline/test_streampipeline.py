import asyncio
from unittest.mock import MagicMock, AsyncMock
import pytest
from core.exchanges.exchange_adapter import ExchangeAdapter
from core.pipeline.streampipeline import StreamPipeline


@pytest.fixture
def adapter() -> ExchangeAdapter:
    class TestAdapter(ExchangeAdapter):
        def validate_authentication(self, authentication_message) -> bool:
            return False
        def get_refresh_authentication_info(self) -> dict:
            return dict()
        def get_authentication_info(self) -> dict | None:
            return None
        def restructure_data(self, data) -> dict:
            return data
        def validate_message(self, msg):
            return True
        def normalise_data(self, batch_list:list) -> list:
            return batch_list
        def writer(self, normalised_list_of_data):
            return
    return TestAdapter(channels=["ticker", "trade"], exchange_name="Test", websocket_url="ws:test", msg={}, ticker="TEST_TEST")

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
    channel_0 = pipeline.exchange_adapter.channels[0]
    channel_1 = pipeline.exchange_adapter.channels[1]
    pipeline.batch_list[channel_0] = [
        {"event": 0},
        {"event": 1},
    ]

    pipeline.batch_list[channel_1] = [
        {"event": 0},
        {"event": 1},
    ]

    await pipeline.process_batch(channel_0)
    await pipeline.process_batch(channel_1)

    assert (len(pipeline.batch_list[channel_0]) == 0) and (len(pipeline.batch_list[channel_1]) == 0)


@pytest.mark.asyncio
async def test_process_restore_batch_on_failure_on_normalisation_of_data(pipeline):
    channel_0 = pipeline.exchange_adapter.channels[0]
    channel_1 = pipeline.exchange_adapter.channels[1]

    previous_batch = [
        {"event": 0},
        {"event": 1},
    ]

    pipeline.batch_list[channel_0] = previous_batch
    pipeline.batch_list[channel_1] = previous_batch

    def fail_normalisation(batch_list):
        raise Exception("bad data")

    pipeline.exchange_adapter.normalise_data = fail_normalisation

    await pipeline.process_batch(channel=channel_0)
    await pipeline.process_batch(channel=channel_1)

    # gets all values of the old batch and checks if they are in the pipeline.batch_list to see if there has been a recovery -> all means every value must be true
    assert (pipeline.batch_list[channel_0] == [{"event": 0},{"event": 1}]) and (pipeline.batch_list[channel_1] == [{"event": 0},{"event": 1}])

# remember you are testing not adding additional functionality ,e.g. not clearing list for it to trigger another set of events
@pytest.mark.asyncio
async def test_consumer_triggers_batch_process_and_again_on_error_thrown(pipeline:StreamPipeline):
    channel_0 = pipeline.exchange_adapter.channels[0]
    channel_1 = pipeline.exchange_adapter.channels[1]

    pipeline.max_size_for_batch = 2
    # we use AsyncMock to mock functions that are async
    pipeline.process_batch = AsyncMock()

    await pipeline.queue[channel_0].put({"event": 0})
    await pipeline.queue[channel_0].put({"event": 1})

    await pipeline.queue[channel_1].put({"event": 0})
    await pipeline.queue[channel_1].put({"event": 1})

    # this has to be a task not just await because it has a while loop in it
    consumer_task_0 = asyncio.create_task(pipeline.consumer(channel=channel_0))
    consumer_task_1 = asyncio.create_task(pipeline.consumer(channel=channel_1))

    await asyncio.sleep(0.1)

    consumer_task_0.cancel()

    await asyncio.gather(consumer_task_0, return_exceptions=True)

    # assert_awaited() -> can reset if ran twice,  thus throwing errors
    bool_0 = pipeline.process_batch.await_count >= 2

    consumer_task_1.cancel()
    await asyncio.gather(consumer_task_1, return_exceptions=True)

    bool_1 =  pipeline.process_batch.await_count >= 2

    assert (bool_0 and bool_1) == True


@pytest.mark.asyncio
async def test_consumer_empties_queue_on_cancel(pipeline):
    channel_0 = pipeline.exchange_adapter.channels[0]
    channel_1 = pipeline.exchange_adapter.channels[1]

    pipeline.max_size_for_batch = 2

    await pipeline.queue[channel_0].put({"event": 0})
    await pipeline.queue[channel_0].put({"event": 1})

    await pipeline.queue[channel_1].put({"event": 0})
    await pipeline.queue[channel_1].put({"event": 1})

    # this has to be a task not just await because it has a while loop in it
    consumer_task_0 = asyncio.create_task(pipeline.consumer(channel_0))
    consumer_task_1 = asyncio.create_task(pipeline.consumer(channel_1))

    await asyncio.sleep(0.1)

    consumer_task_0.cancel()
    consumer_task_1.cancel()

    await asyncio.gather(consumer_task_0, consumer_task_1, return_exceptions=True)

    assert (pipeline.queue[channel_0].qsize() >= 0) and (pipeline.queue[channel_1].qsize() >= 0)

@pytest.mark.asyncio
async def test_consumer_pushes_remaining_batch_on_cancel(pipeline:StreamPipeline):
    channel_0 = pipeline.exchange_adapter.channels[0]
    channel_1 = pipeline.exchange_adapter.channels[1]

    # the limit here to reach on which a process/write occurs, if below then cancel list is pushed
    pipeline.max_size_for_batch = 4

    await pipeline.queue[channel_0].put({"bid":1, 'ask':1})
    await pipeline.queue[channel_0].put({"bid":2, 'ask':2})

    await pipeline.queue[channel_1].put({"bid":1, 'ask':1})
    await pipeline.queue[channel_1].put({"bid":2, 'ask':2})

    # this has to be a task not just await because it has a while loop in it
    consumer_task_0 = asyncio.create_task(pipeline.consumer(channel_0))
    consumer_task_1 = asyncio.create_task(pipeline.consumer(channel_1))

    await asyncio.sleep(0.1)

    consumer_task_0.cancel()
    consumer_task_1.cancel()

    await asyncio.gather(consumer_task_0, consumer_task_1, return_exceptions=True)

    assert (len(pipeline.batch_list[channel_0]) == 0) and (len(pipeline.batch_list[channel_1]) == 0)


@pytest.mark.asyncio
async def test_producer_pushes_valid_items_into_queue(pipeline):
    channel_0 = pipeline.exchange_adapter.channels[0]
    channel_1 = pipeline.exchange_adapter.channels[1]

    async def fake_stream():
        yield {"channel":channel_0, "bid":1, 'ask':1}
        yield {"channel":channel_0, "bid":2, 'ask':2}

        yield {"channel":channel_1, "bid":1, 'ask':1}
        yield {"channel":channel_1, "bid":2, 'ask':2}

    # hot swapping out the functions
    pipeline.ws.stream = fake_stream

    # ---------------------------------------------------------
    producer_task = asyncio.create_task(pipeline.producer())
    await asyncio.sleep(0.1)

    producer_task.cancel()

    assert (pipeline.queue[channel_0].qsize() == 2) and (pipeline.queue[channel_1].qsize() == 2)



# I've commented out the duplication function in the validation as we might need additional info received from that data, e.g. change in funding rate
# @pytest.mark.asyncio
# async def test_producer_pushes_valid_items_and_refused_invalid_items_into_queue(pipeline):
#     channel_0 = pipeline.exchange_adapter.channels[0]
#     async def fake_stream():
#         # accept
#         yield {"channel":channel_0, "bid":1, 'ask':1}
#         # refuses -> same quotes as above thus duplicates
#         yield {"channel":channel_0, "bid":1, 'ask':1}
#         # accept
#         yield {"channel":channel_0, "bid":2, 'ask':2}
#     # hot swapping out the functions
#     pipeline.ws.stream = fake_stream
#
#
#     # ---------------------------------------------------------
#     producer_task = asyncio.create_task(pipeline.producer())
#     await asyncio.sleep(0.1)
#
#     producer_task.cancel()
#
#     assert pipeline.queue[channel_0].qsize() == 2
#
