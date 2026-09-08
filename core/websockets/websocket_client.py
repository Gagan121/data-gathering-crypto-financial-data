import json
import random

import websockets
import asyncio
import time

from core.exchanges.exchange_adapter import ExchangeAdapter


class WebsocketClient:
    def __init__(self, exchange_adapter:ExchangeAdapter):
        self.ws = None
        self.exchange_adapter = exchange_adapter
        self.token_death_timestamp = 0
        # x seconds before token_death_timestamp we revive the token
        self.revive_token_period = 10
        self.connected = asyncio.Event()
        self.next_request_id = 0

    def get_next_request_id(self):
        # this way the request_id starts at 0
        request_id = self.next_request_id
        self.next_request_id += 1
        return request_id


    async def shutdown(self):
        # flags used for concurrency like events
        self.connected.clear()
        if self.ws:
            await self.ws.close()
        self.ws = None

    async def stream(self):
        print(
            f"STREAM STARTED: "
            f"{self.exchange_adapter.get_exchange_name(), "number of channels", len(self.exchange_adapter.channels)} "
            f"{time.time()}",
            flush=True
        )
        # worth noting that scheduled maintenance can knock off connection and cause issues with API rate limiting
        delay = 10
        try:
            while True:
                try:
                    await self.connect()
                    # creates the authentication for a year
                    await self.authenticate()
                    # time.sleep(2)
                    # # creates authentication for 900 seconds
                    # await self.authenticate()
                    await self.sent_msg_to_websocket(self.exchange_adapter.get_data_request_msg())
                    await self.set_heart_beat()
                    async for message in self.ws:

                        if (self.token_death_timestamp != 0) and (time.time() > self.token_death_timestamp):
                            await self.authenticate()

                        data = json.loads(message)
                        sys_time = time.time()
                        data['sys_time'] = sys_time

                        await self.filter_message_and_respond(data)

                        delay = 10

                        yield data
                except Exception as e:
                    delay = min(delay * 2, 30)
                    print(f"Disconnected... {time.time()} \n{e}")
                    self.exchange_adapter.clear_tokens()
                    await asyncio.sleep(delay + random.uniform(0,5))
        except asyncio.CancelledError as e:
            print(f"Closing connection... in websocket, stream() {time.time()} \n{e}")

            print(
                f"STREAM CANCELLED: "
                f"{self.exchange_adapter.get_exchange_name(), "number of channels", len(self.exchange_adapter.channels)} "
                f"{time.time()}",
                flush=True
            )
            await self.shutdown()
            raise

    async def filter_message_and_respond(self, data):

        if type(data) is dict and "method" in data:
            if (data["method"] == "heartbeat"):
                await self.reply_to_heart_beat()


    def validate_authentication(self, authentication_message) -> bool:
        valid = self.exchange_adapter.validate_authentication(authentication_message)
        if valid:
            seconds_time_token_collected = self.exchange_adapter.get_time_token_collected()
            second_till_token_expires = self.exchange_adapter.get_time_token_expires()

            self.token_death_timestamp = seconds_time_token_collected + second_till_token_expires
            self.token_death_timestamp = self.token_death_timestamp - self.revive_token_period

        return valid

    async def connect(self):

        self.ws = await websockets.connect(self.exchange_adapter.get_websocket_url())
        self.connected.set()


    async def sent_msg_to_websocket(self, msg):

        await self.connected.wait()

        if self.ws is None:
            raise RuntimeError("websocket is not connected")

        if msg is None:
            raise ValueError("message cannot be None")

        request_id = self.get_next_request_id()
        new_msg = self.exchange_adapter.add_request_id(msg=msg, request_id=request_id)

        if new_msg is None:
            raise ValueError("new_msg, data given from add_request_id has returned None, if no id value can be added then return the original message")

        try:
            await self.ws.send(json.dumps(new_msg))
        except Exception as e:
            print(e)


    async def recv_message_from_websocket(self):
        return await self.ws.recv()

    async def set_heart_beat(self):
        if not (self.exchange_adapter.get_heart_beat_msg() is None):
            await self.sent_msg_to_websocket(self.exchange_adapter.get_heart_beat_msg())



    async def reply_to_heart_beat(self):
        if not (self.exchange_adapter.get_heart_beat_reply_msg() is None):
            await self.sent_msg_to_websocket(self.exchange_adapter.get_heart_beat_reply_msg())


    # only work because we authenticate before requesting data
    async def authenticate(self):
        if not (self.exchange_adapter.get_authentication_info() is None):

            if self.exchange_adapter.if_refresh_token_exists():
                await self.sent_msg_to_websocket(self.exchange_adapter.get_refresh_authentication_info())
            else:
                await self.sent_msg_to_websocket(self.exchange_adapter.get_authentication_info())

            authentication_message = json.loads(await self.recv_message_from_websocket())

            if not (self.validate_authentication(authentication_message)):
                raise ValueError("error in authentication")


