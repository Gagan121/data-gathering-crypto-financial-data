from tests.websockets.fake_websocket import FakeWebsocket


class FlakyConnector:
    def __init__(self):
        self.calls = 0
    # this allows a object to be called as a function e.g. flaky_connector() can be ran with no functions tied to it just object() with brackets
    async def __call__(self, *args, **kwargs):
        self.calls += 1

        if self.calls == 1:
            raise Exception("connection failed")

        return FakeWebsocket(['{"event":1}'])