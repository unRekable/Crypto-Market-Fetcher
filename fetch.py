import ccxt
import pyarrow.parquet as pq
import pyarrow as pa
import pyarrow
import pandas as pd
import time
import argparse
import logging
from datetime import datetime

# Binance API credentials
binance_api_key = 'YOUR_BINANCE_API_KEY'
binance_api_secret = 'YOUR_BINANCE_API_SECRET'

# Coinbase API credentials
coinbase_api_key = 'YOUR_COINBASE_API_KEY'
coinbase_api_secret = 'YOUR_COINBASE_API_SECRET'
coinbase_passphrase = 'YOUR_COINBASE_PASSPHRASE'

# Bitget API credentials
bitget_api_key = 'YOUR_BITGET_API_KEY'
bitget_api_secret = 'YOUR_BITGET_API_SECRET'

# Initialize Binance exchange
binance_exchange = ccxt.binance({
    'apiKey': binance_api_key,
    'secret': binance_api_secret,
    'enableRateLimit': True,
})

# Initialize Coinbase exchange
coinbase_exchange = ccxt.coinbasepro({
    'apiKey': coinbase_api_key,
    'secret': coinbase_api_secret,
    'password': coinbase_passphrase,
    'enableRateLimit': True,
})

# Initialize Bitget exchange
bitget_exchange = ccxt.bitget({
    'apiKey': bitget_api_key,
    'secret': bitget_api_secret,
    'enableRateLimit': True,
})

# Function to fetch OHLCV data and save it to Parquet and CSV files
def fetch_ohlcv_and_save(exchange, symbol, start_timestamp, end_timestamp, timeframe):
    while True:
        try:
            since = start_timestamp
            ohlcv = []
            
            while since <= end_timestamp:
                print(f"Fetching OHLCV data for {symbol} - Timeframe: {timeframe} - Since: {datetime.fromtimestamp(since / 1000).strftime('%Y-%m-%d %H:%M:%S')}")
                new_ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since=since, limit=500)
                if not new_ohlcv:
                    break
                ohlcv.extend(new_ohlcv)
                since = new_ohlcv[-1][0] + 1

            ohlcv_df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

            base_filename = f'{exchange.id}_{symbol.replace("/", "_")}_{timeframe}_ohlcv'
            parquet_file_path = f'{base_filename}.parquet'
            csv_file_path = f'{base_filename}.csv'

            print(f"Fetching data since: {pd.to_datetime(start_timestamp, unit='ms')}")
            table = pq.write_table(pa.Table.from_pandas(ohlcv_df), parquet_file_path)
            ohlcv_df.to_csv(csv_file_path, index=False)
            print(f"Saved OHLCV data to {parquet_file_path} and {csv_file_path}")
            break
        except ccxt.RequestTimeout as e:
            print(f"Rate limit exceeded for {exchange.id}. Pausing for {e.timeout / 1000} seconds.")
            time.sleep(e.timeout / 1000)

# Function to fetch order book data and save it to Parquet and CSV files
def fetch_orderbook_and_save(exchange, symbol):
    while True:
        try:
            orderbook = exchange.fetch_order_book(symbol)
            orderbook_df = pd.DataFrame(orderbook['bids'] + orderbook['asks'], columns=['price', 'quantity'])
            orderbook_df['timestamp'] = pd.to_datetime('now')  # Add current timestamp column
            base_filename = f'{exchange.id}_{symbol.replace("/", "_")}_orderbook'
            parquet_file_path = f'{base_filename}.parquet'
            csv_file_path = f'{base_filename}.csv'

            print(f"Fetching order book data for {symbol}")
            table = pq.write_table(pa.Table.from_pandas(orderbook_df), parquet_file_path)
            orderbook_df.to_csv(csv_file_path, index=False)
            print(f"Saved order book data to {parquet_file_path} and {csv_file_path}")
            break
        except ccxt.RequestTimeout as e:
            print(f"Rate limit exceeded for {exchange.id}. Pausing for {e.timeout / 1000} seconds.")
            time.sleep(e.timeout / 1000)

# Function to fetch ticker data and save it to Parquet and CSV files
def fetch_ticker_and_save(exchange, symbol):
    while True:
        try:
            ticker = exchange.fetch_ticker(symbol)
            ticker_df = pd.DataFrame([ticker], columns=['symbol', 'timestamp', 'datetime', 'high', 'low', 'bid', 'ask', 'last', 'baseVolume', 'quoteVolume'])
            ticker_df['timestamp'] = pd.to_datetime('now')  # Add current timestamp column
            base_filename = f'{exchange.id}_{symbol.replace("/", "_")}_ticker'
            parquet_file_path = f'{base_filename}.parquet'
            csv_file_path = f'{base_filename}.csv'

            print(f"Fetching ticker data for {symbol}")
            table = pq.write_table(pa.Table.from_pandas(ticker_df), parquet_file_path)
            ticker_df.to_csv(csv_file_path, index=False)
            print(f"Saved ticker data to {parquet_file_path} and {csv_file_path}")
            break
        except ccxt.RequestTimeout as e:
            print(f"Rate limit exceeded for {exchange.id}. Pausing for {e.timeout / 1000} seconds.")
            time.sleep(e.timeout / 1000)

# Main function to run the script
def main(exchange_name, symbol, start_date, end_date, timeframe, market_type, data_type):
    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')   
    logging.info(f"Fetching data for {symbol} - Start Date: {start_date}, End Date: {end_date}, Timeframe: {timeframe}, Market Type: {market_type}, Data Type: {data_type}")

    exchange = None
    if exchange_name == 'binance':
        exchange = binance_exchange
        if market_type == 'perpetuals':
            exchange = ccxt.binanceusdm()  # Use binanceusdm for perpetuals market
    elif exchange_name == 'coinbase':
        exchange = coinbase_exchange
    elif exchange_name == 'bitget':
        exchange = bitget_exchange
    else:
        print(f"Unsupported exchange: {exchange_name}")
        return
    
    start_timestamp = int(time.mktime(time.strptime(start_date, "%Y-%m-%d"))) * 1000
    end_timestamp = int(time.mktime(time.strptime(end_date, "%Y-%m-%d"))) * 1000 + 24 * 60 * 60 * 1000 - 1
    
    if data_type == 'ohlcv' or data_type == 'all':
        fetch_ohlcv_and_save(exchange, symbol, start_timestamp, end_timestamp, timeframe)
    if data_type == 'orderbook' or data_type == 'all':
        fetch_orderbook_and_save(exchange, symbol)
    if data_type == 'ticker' or data_type == 'all':
        fetch_ticker_and_save(exchange, symbol)    

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Fetch and save market data')
    parser.add_argument('-exchange', type=str, required=True, help='Exchange name (binance, coinbase, bitget)')
    parser.add_argument('-symbol', type=str, required=True, help='Trading symbol (e.g., BTC/USDT)')
    parser.add_argument('-start_date', type=str, required=True, help='Start date in YYYY-MM-DD format')
    parser.add_argument('-end_date', type=str, required=True, help='End date in YYYY-MM-DD format')
    parser.add_argument('-timeframe', type=str, required=True, help='Timeframe for OHLCV data (e.g., 1m)')
    parser.add_argument('-market_type', type=str, required=True, choices=['spot', 'perpetuals'], help='Market type (spot or perpetuals)')
    parser.add_argument('-data_type', type=str, required=True, choices=['ohlcv', 'orderbook', 'ticker', 'all'], help='Type of data to fetch')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    main(args.exchange, args.symbol, args.start_date, args.end_date, args.timeframe, args.market_type, args.data_type)
