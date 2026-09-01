import asyncio
import time
import pytest
from core.rest_requests.rest_client_requests import RestClient

@pytest.fixture
def rest_client() -> RestClient:
    return RestClient()


def test_curl_calls_can_be_made(rest_client):
    base_url = "https://www.deribit.com/api/v2/public/get_time"
    msg = {}
    data = rest_client.get_request(full_url=base_url, msg=msg)

    if (isinstance(data, dict)) and ("result" in data):
        # now converted to number of seconds
        diff_in_time = abs(int(data["result"]) - int(time.time() * 1_000)) / 1_000
        assert (diff_in_time < 5)
        return

    pytest.fail("data object was not correctly made")
