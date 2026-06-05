import asyncio

from core.exchanges.coinbase_adapter import CoinbaseAdapter
from core.pipeline.streampipeline import StreamPipeline


async def main():
    coinbase_adapter = CoinbaseAdapter(
        exchange_name="Coinbase",
        path_to_folder=r"D:\python_projects\data_gathering_using_websockets_finance_crypto\data",
        url="wss://advanced-trade-ws.coinbase.com",
        msg= {
            "type": "subscribe",
            "product_ids": ["BTC-USD"],
            "channel": "ticker"
        }
    )

    stream_pipeline = StreamPipeline(exchange_adapter=coinbase_adapter)

    await asyncio.gather(
        stream_pipeline.run()
    )




if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt as e:
        print("Keyboard interrupt: ",e)
