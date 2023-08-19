# Crypto Market Data Fetcher

This script fetches and stores cryptocurrency market data from popular exchanges such as Binance, Coinbase, and Bitget. It allows you to retrieve and save historical OHLCV data, order book data, ticker data, and trade data. You can specify the exchange, trading symbol, date range, timeframe, market type, and data type to fetch.

## Features

- Fetch and save OHLCV (Open, High, Low, Close, Volume) data for a trading pair.
- Fetch and save order book data (bids and asks) for a trading pair.
- Fetch and save ticker data (last price, bid, ask, volume, etc.) for a trading pair.
- Fetch and save trade data (timestamp, price, amount, side) for a trading pair.
- Supports spot markets and perpetuals (futures) markets.
- Option to resume fetching `trades` data from the last progress.
- Data is saved in both Parquet and CSV formats for easy analysis.

## Prerequisites

- Python 3.x
- CCXT library (`pip install ccxt`)
- PyArrow library (`pip install pyarrow`)
- Pandas library (`pip install pandas`)

Make sure to have these libraries installed before running the script.

## Usage

1. Clone this repository to your local machine.
2. Install the required Python libraries using the following command:
   ```bash
   pip install ccxt pyarrow pandas
   ```
3. Update the API credentials in the script for Binance, Coinbase, and Bitget.
4. Run the `fetch.py` script with the following command-line arguments:

```bash
python3 fetch.py -exchange <exchange_name> -symbol <trading_symbol> -start_date <start_date> -end_date <end_date> -timeframe <timeframe> -market_type <market_type> -data_type <data_type> [-resume]
```

## Command-line Arguments

To use the script, you need to provide the following arguments:

- `-exchange`: Name of the exchange (e.g., binance, coinbase, bitget).
- `-symbol`: Trading symbol (e.g., BTC/USDT).
- `-start_date`: Start date in YYYY-MM-DD format.
- `-end_date`: End date in YYYY-MM-DD format.
- `-timeframe`: Timeframe for OHLCV data (e.g., 1m, 1h, 1d).
- `-market_type`: Market type (spot or perpetuals).
- `-data_type`: Data type to fetch (ohlcv, orderbook, ticker, trades).
- `-resume` (optional): Resume fetching `trades` data from the last progress.

Example command to fetch spot market OHLCV data for BTC/USDT from Binance:

```bash
python3 fetch.py -exchange binance -symbol BTC/USDT -start_date 2020-01-01 -end_date 2023-08-01 -timeframe 1h -market_type spot -data_type ohlcv
```

Example command to fetch perpetuals (futures) trade data for BTC/USDⓈ-M from Binance:

```bash
python3 fetch.py -exchange binance -symbol BTC/USDT -start_date 2020-01-01 -end_date 2023-08-01 -timeframe 1h -market_type perpetuals -data_type trades
```

## Supported Exchanges

The script currently supports the following cryptocurrency exchanges:

- Binance (Spot and Perpetuals)
- Coinbase Pro
- Bitget (Spot and Perpetuals)

## Notes

- You need to provide your API credentials for the supported exchanges in the `fetch.py` script.
- For `trades` data, fetched trades are saved directly to the CSV file and can be resumed from the last progress, which is tracked in a progress file. The conversion to Parquet format is performed at the end of the `trades` retrieval. The progress file is automatically removed upon successful completion.
- The fetched data will be saved as Parquet and CSV files in the current directory.
- The script will handle rate limits and automatically pause when needed.

## Contributing

Contributions are welcome! If you find any issues or have suggestions for improvements, feel free to create an issue or submit a pull request.
