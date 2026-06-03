from asyncio import to_thread

from websockets.asyncio.client import connect
import asyncio
import json
from datetime import datetime
import time
import pandas as pd

queue = asyncio.Queue(maxsize=1000)

def validate_message(msg):
    return "events" in msg and msg["events"]

def normalise_data(data_lot:list) -> list:
    normalised_data = []
    for data in data_lot:
        ts = data["timestamp"]
        sys_time = data['sys_time']
        new_timestamp_numerical = datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
        exch_ts_sec, exch_ts_micro = [int(item) for item in str(new_timestamp_numerical).split('.')]
        sys_ts_sec, sys_ts_micro = [int(item) for item in str(round(sys_time, 6)).split('.')]

        try:
            price = data['events'][0]['tickers'][0]['price']
            bid =  data['events'][0]['tickers'][0]['best_bid']
            ask = data['events'][0]['tickers'][0]['best_ask']
            bid_quantity = data['events'][0]['tickers'][0]['best_bid_quantity']
            ask_quantity = data['events'][0]['tickers'][0]['best_ask_quantity']
        except (KeyError, IndexError, TypeError):
            continue


        norm_data = {
            'exch_ts_sec': exch_ts_sec,
            'exch_ts_micro': exch_ts_micro,
            'sys_ts_sec': sys_ts_sec,
            'sys_ts_micro': sys_ts_micro,
            'price': price,
            'bid': bid,
            'ask': ask,
            'bid_quantity': bid_quantity,
            'ask_quantity': ask_quantity,
        }

        normalised_data.append(norm_data)

    return normalised_data


def writer(normalised_list_of_data:list, filename:str):
    df = pd.DataFrame(normalised_list_of_data)
    path_to_folder = fr"D:\python_projects\data_gathering_using_websockets_finance_crypto\coinbase_data\{filename}"
    df.to_parquet(path=path_to_folder)

async def consumer():
    batch_data = []
    while True:
        mes = await queue.get()

        batch_data.append(mes)

        if len(batch_data) >= (queue.maxsize - 1):
            normalised_list_of_data = normalise_data(batch_data)
            filename = f"data_coinbase_{int(time.time())}.parquet"
            await asyncio.to_thread(
                writer,
                normalised_list_of_data,
                filename
            )
            batch_data.clear()
        # tells us the work item has been processed
        queue.task_done()


async def producer():
    delay = 1
    while True:
        try:
            async with connect("wss://advanced-trade-ws.coinbase.com") as websocket:
                subscribe_message = {
                    "type": "subscribe",
                    "product_ids": ["BTC-USD"],
                    "channel": "ticker"
                }
                await websocket.send(json.dumps(subscribe_message))

                delay = 1

                async for message in websocket:
                    data = json.loads(message)
                    if validate_message(data):
                        data['sys_time'] = time.time()
                        await queue.put(data)
        except Exception as e:
            print(f"Disconnected... {time.time()} \n{e}")
            await asyncio.sleep(delay)
            # increasing delay, by a factor of 2 and till get gets to the limit of 30 seconds and stays there
            delay = min(delay * 2, 30)
                # print("test")
    #             need to pass this into a async queue and then saved to file




async def main():
    await asyncio.gather(
        producer(),
        consumer()
    )

asyncio.run(main())
