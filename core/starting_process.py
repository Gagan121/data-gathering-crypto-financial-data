import asyncio

from core.exchanges.binance_adapter import BinanceAdapter
from core.exchanges.coinbase_adapter import CoinbaseAdapter
from core.pipeline.streampipeline import StreamPipeline


async def main():
    coinbase_adapter = CoinbaseAdapter(
        exchange_name="Coinbase",
        ticker="BTC_USD",
        path_to_folder=r"D:\python_projects\data_gathering_using_websockets_finance_crypto\data",
        url="wss://advanced-trade-ws.coinbase.com",
        msg= {
            "type": "subscribe",
            "product_ids": ["BTC-USD"],
            "channel": "ticker"
        }
    )



    binance_adapter = BinanceAdapter(
        exchange_name="Binance",
        ticker="BTC_USDT",
        path_to_folder=r"D:\python_projects\data_gathering_using_websockets_finance_crypto\data",
        url="wss://fstream.binance.com/public/ws/btcusdt@bookTicker",
        # this is required otherwise it throw error of blank message being sent -> after a couple of seconds throw error of invalid request
        msg= {
            "method": "SUBSCRIBE",
            "params":
            [
                "btcusdt@bookTicker"
            ],
            "id": 1
        }
    )


    # stream_pipeline = StreamPipeline(exchange_adapter=coinbase_adapter)
    stream_pipeline = StreamPipeline(exchange_adapter=binance_adapter)

    await asyncio.gather(
        stream_pipeline.run()
    )




if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt as e:
        print("Keyboard interrupt: ",e)
