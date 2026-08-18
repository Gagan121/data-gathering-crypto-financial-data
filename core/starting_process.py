import asyncio
import signal
import sys

from core.exchanges.binance_adapter import BinanceAdapter
from core.exchanges.coinbase_adapter import CoinbaseAdapter
from core.pipeline.streampipeline import StreamPipeline


async def main():
    coinbase_adapter = CoinbaseAdapter(
        exchange_name="Coinbase",
        ticker="BTC_USD",
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



    if sys.platform != "win32":
        loop = asyncio.get_running_loop()
        # gets the the current corroutine in the main() function which is the main corroutine tak
        main_task = asyncio.current_task()

        def shutdown_task():
            print("Shutdown signal received", flush=True)
            main_task.cancel()
        # these are need to catch the closing signal from the container or end code
        loop.add_signal_handler(signal.SIGTERM, shutdown_task)
        loop.add_signal_handler(signal.SIGINT, shutdown_task)

    stream_pipeline_0 = StreamPipeline(exchange_adapter=coinbase_adapter)
    stream_pipeline_1 = StreamPipeline(exchange_adapter=binance_adapter)

    await asyncio.gather(
        stream_pipeline_0.run(),
        stream_pipeline_1.run()
    )




if __name__ == '__main__':
    try:
        print("STARTING PYTHON APPLICATION", flush=True)
        asyncio.run(main())
    except Exception as e:
        print("Keyboard interrupt: ",e)

    finally:
        print("PYTHON APPLICATION EXITING", flush=True)
