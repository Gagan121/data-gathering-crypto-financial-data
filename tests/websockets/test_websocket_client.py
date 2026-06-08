import pytest
import json
import asyncio
from tests.websockets.fake_websocket import FakeWebsocket
from core.websockets.websocket_client import WebsocketClient
from unittest.mock import AsyncMock, patch

from tests.websockets.flaky_connector import FlakyConnector


@pytest.mark.asyncio
async def test_stream_with_fake_websocket():

    fake_messages = [
        json.dumps({"event": 1}),
        json.dumps({"event": 2})
    ]

    fake_ws = FakeWebsocket(fake_messages)


    # we are saying here to use AsyncMock fakews for the path to the function below
    # this is used to control the object before its built, whereas, just assigning the function can only be used after the object is made
    with patch(
        # the path is to the dependency in the file not the custom class I made
        "core.websockets.websocket_client.websockets.connect",
        new=AsyncMock(return_value=fake_ws)
    ):
        client = WebsocketClient(url="ws://test", msg={})

        results = []

        async def run():
            async for msg in client.stream():
                results.append(msg)
                if len(results) == 2:
                    break

        await asyncio.wait_for(run(), timeout=1)

        assert results[0]["event"] == 1
        assert results[1]["event"] == 2

@pytest.mark.asyncio
async def test_websocket_close_on_shutdown():
    fake_ws = FakeWebsocket([])
    fake_ws.close = AsyncMock()

    # we are saying here to use AsyncMock fakews for the path to the function below
    # this is used to control the object before its built, whereas, just assigning the function can only be used after the object is made
    with patch(
        # the path is to the dependency in the file not the custom class I made
        "core.websockets.websocket_client.websockets.connect",
        new=AsyncMock(return_value=fake_ws)
    ):
        client = WebsocketClient(url="ws://test", msg={})
        await client.connect()
        await client.shutdown()

        fake_ws.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_reconnect_after_failure():
    flaky_connector = FlakyConnector()

    with patch(
        "core.websockets.websocket_client.websockets.connect",
        new=flaky_connector
    ):
        client = WebsocketClient(url="ws://test", msg={})

        results = []

        async def run():
            async for msg in client.stream():
                results.append(msg)
                if len(results) == 1:
                    break

        # this needs to have a large timeout time otherwise the first error caused by flaky_connector will cause the cause a cancellation error due to it timing out
        await asyncio.wait_for(run(), timeout=10)

    assert len(results) == 1
    assert flaky_connector.calls >= 2


