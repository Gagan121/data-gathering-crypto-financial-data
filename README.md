# Crypto Market Data Collection Pipeline

A real-time cryptocurrency market data collection pipeline built using Python, `asyncio`, and WebSockets.

The application connects to exchange WebSocket feeds, validates incoming market data, buffers messages using an asynchronous producer-consumer architecture, normalises exchange-specific formats, and stores the results as Parquet files.

---

## Features

* Real-time market data ingestion
* Automatic WebSocket reconnection with exponential backoff
* Producer-consumer architecture using `asyncio.Queue`
* Backpressure protection using bounded queues
* Batched data processing
* Exchange adapter pattern for multi-exchange support
* Parquet file storage
* Graceful shutdown handling
* Unit testing with `pytest`

---

## Architecture

```text
WebSocket Feed
       │
       ▼
   Producer
       │
       ▼
  Async Queue
       │
       ▼
   Consumer
       │
       ▼
 Batch Buffer
       │
       ▼
 Data Normalisation
       │
       ▼
  Parquet Writer
```

### Producer

The producer continuously receives messages from an exchange WebSocket feed.

Validated messages are placed onto a bounded queue.

### Queue

The queue acts as a backpressure mechanism.

### Consumer

The consumer removes messages from the queue and accumulates them into batches.

### Batch Processing

Once a batch reaches the configured size, messages are:

1. Normalised into a common schema
2. Converted into a Pandas DataFrame
3. Written to disk as a Parquet file

### Storage

Data is stored in exchange-specific folders:

```text
data/
└── Coinbase/
    ├── data_Coinbase_BTC_USD_1749200000.parquet
    ├── data_Coinbase_BTC_USD_1749205000.parquet
    └── ...
```

---

## Project Structure

```text
core/
├── exchanges/
│   ├── exchange_adapter.py
│   ├── coinbase_adapter.py
|   └── <exchange_name>_adapter.py
│
├── pipeline/
│   └── streampipeline.py
│
├── websockets/
│   └── websocket_client.py
│
└── starting_process.py

tests/
```

### Exchange Adapters

Exchange-specific logic is isolated using the adapter pattern.

Each adapter is responsible for:

* Message validation
* Data normalisation
* Exchange-specific configuration

Example:

```python
class CoinbaseAdapter(ExchangeAdapter):
    def validate_message(self, msg):
        ...

    def normalise_data(self, batch_list):
        ...
```

This allows additional exchanges to be added without modifying the pipeline implementation.

---

## Current Exchange Support

| Exchange                | Status    |
| ----------------------- | --------- |
| Coinbase Advanced Trade | Supported |
| Binance                 | Supported |
| Kraken                  | Planned   |
| Bybit                   | Planned   |

---

## Installation

Clone the repository:

```bash
git clone <repository-url>
cd <repository-name>
```

Create a virtual environment:

```bash
python -m venv venv
```

Activate the environment:

### Windows

```bash
venv\Scripts\activate
```

### Linux / macOS

```bash
source venv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

---

## Running the Application

```bash
python -m core.starting_process
```

The application will:

1. Connect to the configured exchange
2. Subscribe to the desired market data channel
3. Buffer incoming messages
4. Batch and normalise data
5. Persist results as Parquet files

---

## Example Output Schema

```python
{
    "exch_ts_sec": 1749200000,
    "exch_ts_micro": 123456,
    "sys_ts_sec": 1749200000,
    "sys_ts_micro": 654321,
    "price": "108000.00",
    "bid": "107999.50",
    "ask": "108000.50",
    "bid_quantity": "0.52",
    "ask_quantity": "0.31"
}
```

---

## Design Decisions

### Why use a bounded queue?

A bounded queue prevents unbounded memory growth during periods where incoming market data exceeds processing throughput.

```python
self.queue = asyncio.Queue(maxsize=1000)
```

### Why batch writes?

Batching reduces write frequency and improves throughput.

### Why Parquet?

Parquet provides:

* Efficient columnar storage
* Compression
* Fast analytical reads
* Native Pandas support

### Why use an adapter pattern?

Different exchanges expose different message formats.

Adapters isolate exchange-specific logic while allowing the pipeline to remain exchange-agnostic.

---

## Testing

Run the test suite:

```bash
pytest
```

Example areas covered by tests:

* WebSocket connection handling
* Producer-consumer behaviour
* Queue backpressure
* Batch processing
* Parquet writing
* Shutdown behaviour
* Failure recovery

---

## Future Improvements

* Additional exchange support
* Dedicated writer worker
* Monitoring and metrics
* Configuration management
* Database storage backends
* Partitioned Parquet datasets
* Data quality validation
* Docker deployment
