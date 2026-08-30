import asyncio
import signal
import sys

from core.exchanges.binance_adapter import BinanceAdapter
from core.exchanges.coinbase_adapter import CoinbaseAdapter
# from core.exchanges.deribit_perpetuals_adapter import DeribitPerpetualAdapter
from core.exchanges.deribit_options_adapter import DeribitOptionsAdapter
from core.exchanges.exchange_adapter import ExchangeAdapter
from core.pipeline.streampipeline import StreamPipeline


async def main():

    deribit_option_adapters = DeribitOptionsAdapter.generate_deribit_option_adapters(
        interval_type="agg2",
        currency="BTC",
        expired="false",
        data_types=["ticker", "trades"],
        exchange_name="Deribit_Options",
        websocket_url="wss://www.deribit.com/ws/api/v2",
        base_url = "https://test.deribit.com/api/v2/",
        # url="wss://test.deribit.com/ws/api/v2",
        msg={
            "jsonrpc": "2.0",
            "method": "public/subscribe",
            "id": 42,
            "params": {
                "channels": []
            }
        },
        heart_beat_msg={
            "jsonrpc": "2.0",
            "id": 10,
            "method": "public/set_heartbeat",
            "params": {"interval": 30}
        },
        heart_beat_reply_msg={
            "jsonrpc": "2.0",
            "method": "public/test",
            "params": {},
            "id": 1
        }
    )



    # deribit_adapter = DeribitPerpetualAdapter(
    #     channels = ["trades.BTC-PERPETUAL.raw","ticker.BTC-PERPETUAL.raw",],
    #     exchange_name="Deribit",
    #     ticker="BTC_PERPETUAL",
    #     url="wss://www.deribit.com/ws/api/v2",
    #     # url="wss://test.deribit.com/ws/api/v2",
    #     msg={
    #         "jsonrpc": "2.0",
    #         "method": "public/subscribe",
    #         "id": 42,
    #         "params": {
    #             "channels": [
    #                 "trades.BTC-PERPETUAL.raw","ticker.BTC-PERPETUAL.raw"
    #             ]
    #         }
    #     },
    #     heart_beat_msg={
    #         "jsonrpc": "2.0",
    #         "id": 10,
    #         "method": "public/set_heartbeat",
    #         "params": {"interval": 30}
    #     },
    #     heart_beat_reply_msg={
    #         "jsonrpc": "2.0",
    #         "method": "public/test",
    #         "params": {},
    #         "id": 1
    #     }
    # )



    #
    # coinbase_adapter = CoinbaseAdapter(
    #     exchange_name="Coinbase",
    #     ticker="BTC_USD",
    #     url="wss://advanced-trade-ws.coinbase.com",
    #     msg= {
    #         "type": "subscribe",
    #         "product_ids": ["BTC-USD"],
    #         "channel": "ticker"
    #     }
    # )
    # binance_adapter = BinanceAdapter(
    #     exchange_name="Binance",
    #     ticker="BTC_USDT",
    #     url="wss://fstream.binance.com/public/ws/btcusdt@bookTicker",
    #     # this is required otherwise it throw error of blank message being sent -> after a couple of seconds throw error of invalid request
    #     msg= {
    #         "method": "SUBSCRIBE",
    #         "params":
    #         [
    #             "btcusdt@bookTicker"
    #         ],
    #         "id": 1
    #     }
    # )



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

    # stream_pipeline_0 = StreamPipeline(exchange_adapter=coinbase_adapter)
    # stream_pipeline_1 = StreamPipeline(exchange_adapter=binance_adapter)
    # stream_pipeline_2 = StreamPipeline(exchange_adapter=deribit_adapter)
    # True if information is there
    if not (bool(deribit_option_adapters)):
        return

    pipelines = [
        StreamPipeline(adapter) for adapter in deribit_option_adapters
    ]
    await asyncio.gather(
        # stream_pipeline_0.run(),
        # stream_pipeline_1.run(),
        # stream_pipeline_2.run()
        # stream_pipeline_3.run()
        *(pipeline.run() for pipeline in pipelines),
    )




if __name__ == '__main__':
    try:
        print("STARTING PYTHON APPLICATION", flush=True)
        asyncio.run(main())
    except Exception as e:
        print("Keyboard interrupt: ",e)

    finally:
        print("PYTHON APPLICATION EXITING", flush=True)
