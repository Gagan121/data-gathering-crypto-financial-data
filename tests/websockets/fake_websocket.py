
class FakeWebsocket:

    def __init__(self, messages:list):
        self.messages = iter(messages)
        self.closed = False
        self.sent = None

    async def send(self, msg):
        self.sent = msg

    async def close(self):
        self.closed = True

    # below are used for async for loops yield
    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self.messages)
        except StopIteration:
            raise StopAsyncIteration
