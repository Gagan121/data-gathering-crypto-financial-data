import json
import websockets
import asyncio
import time

from pandas.core.dtypes.astype import astype_array

from core.exchanges.exchange_adapter import ExchangeAdapter


class WebsocketClient:
    def __init__(self, exchange_adapter:ExchangeAdapter):
        self.ws = None
        self.exchange_adapter = exchange_adapter
        self.token_death_timestamp = 0
        # x seconds before token_death_timestamp we revive the token
        self.revive_token_period = 10


    async def shutdown(self):
        if self.ws:
            await self.ws.close()

    async def stream(self):
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
                    await asyncio.sleep(delay)
        except asyncio.CancelledError as e:
            print(f"Closing connection... in websocket, stream() {time.time()} \n{e}")
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

    async def send_message_through_websocket_and_receive_message(self, msg):
        if not (msg is None):
            try:
                await self.sent_msg_to_websocket(msg)
                response = json.loads(await self.recv_message_from_websocket())
                print(response)
            except Exception as e:
                print(e)

        return None


    async def connect(self):
        self.ws = await websockets.connect(self.exchange_adapter.get_websocket_url())
    # we can leave msg a parameter here if a additional message is required in the future
    async def sent_msg_to_websocket(self, msg):
        await self.ws.send(json.dumps(msg))

    async def recv_message_from_websocket(self):
        return await self.ws.recv()

    async def set_heart_beat(self):
        if not (self.exchange_adapter.get_heart_beat_msg() is None):
            await self.sent_msg_to_websocket(self.exchange_adapter.get_heart_beat_msg())

        # heart_beat_response = json.loads(await self.recv_message_from_websocket())
        # print(heart_beat_response)


    async def reply_to_heart_beat(self):
        if not (self.exchange_adapter.get_heart_beat_reply_msg() is None):
            await self.sent_msg_to_websocket(self.exchange_adapter.get_heart_beat_reply_msg())

        # reply_to_heart_beat_response = json.loads(await self.recv_message_from_websocket())
        # print(reply_to_heart_beat_response)


    async def authenticate(self):
        if not (self.exchange_adapter.get_authentication_info() is None):

            if self.exchange_adapter.if_refresh_token_exists():
                await self.sent_msg_to_websocket(self.exchange_adapter.get_refresh_authentication_info())
            else:
                await self.sent_msg_to_websocket(self.exchange_adapter.get_authentication_info())

            authentication_message = json.loads(await self.recv_message_from_websocket())

            if not (self.validate_authentication(authentication_message)):
                raise ValueError("error in authentication")


