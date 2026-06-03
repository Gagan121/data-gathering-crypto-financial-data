import asyncio

from core.exchanges.coinbase_adapter import CoinbaseAdapter
from core.pipeline.streampipeline import StreamPipeline


def main():
    coinbase_adapter = CoinbaseAdapter(
        path_to_folder=r"D:\python_projects\data_gathering_using_websockets_finance_crypto\data",
        url="wss://advanced-trade-ws.coinbase.com",
        msg= {
            "type": "subscribe",
            "product_ids": ["BTC-USD"],
            "channel": "ticker"
        }
    )

    stream_pipeline = StreamPipeline(exchange_adapter=coinbase_adapter)

    asyncio.gather(
        stream_pipeline.run()
    )




if __name__ == '__main__':
    main()