import ccxt
import pyarrow.parquet as pq
import pyarrow as pa
import pyarrow
import pandas as pd
import time
import argparse
import logging
import json 
import os
import csv
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

# Function to convert CSV to Parquet
def convert_csv_to_parquet(csv_file_path):
    df = pd.read_csv(csv_file_path)
    parquet_file_path = csv_file_path.replace('.csv', '.parquet')
    table = pq.write_table(pa.Table.from_pandas(df), parquet_file_path)
    return parquet_file_path

# Function to fetch trade data and save it to CSV file
def fetch_trades_and_save(exchange, symbol, start_timestamp, end_timestamp, resume=False):
    # Format start and end dates
    start_date = pd.to_datetime(start_timestamp, unit='ms').strftime('%Y-%m-%d')
    end_date = pd.to_datetime(end_timestamp, unit='ms').strftime('%Y-%m-%d')

    # Progress file to track the last fetched trade
    progress_file = f'{exchange.id}_{symbol.replace("/", "_")}_progress_{start_date}_{end_date}.json'

    # Initialize 'since' timestamp
    since = start_timestamp
    one_hour = 3600 * 1000
    previous_trade_id = None

    # Check if resume mode is enabled
    if resume:
        resume_progress = None
        if os.path.isfile(progress_file):
            with open(progress_file, 'r') as progress_json:
                resume_progress = json.load(progress_json)
        if resume_progress and resume_progress.get('symbol') == symbol:
            since = resume_progress.get('progress', start_timestamp)
            print(f"Resumed from progress. Start from timestamp: {since}")
        else:
            print("Progress data does not match symbol or does not exist. Starting from the beginning.")
    else:
        print("Starting from the beginning...")    

    # Prepare CSV file
    csv_file_path = f'{exchange.id}_{symbol.replace("/", "_")}_trades_{start_date}_{end_date}.csv'
    write_header = not os.path.isfile(csv_file_path)

    while since < end_timestamp:
        try:
            # Fetch trades since the 'since' timestamp
            fetched_trades = exchange.fetch_trades(symbol, since)
            print(exchange.iso8601(since), len(fetched_trades), 'trades')
            if len(fetched_trades):
                last_trade = fetched_trades[-1]
                if previous_trade_id != last_trade['id']:
                    since = last_trade['timestamp']
                    previous_trade_id = last_trade['id']

                    # Write fetched trades to CSV
                    with open(csv_file_path, mode='a', newline='') as csv_f:
                        csv_writer = csv.DictWriter(csv_f, delimiter=",", fieldnames=["timestamp", "price", "amount", "side"])
                        if write_header:
                            csv_writer.writeheader()
                            write_header = False
                        for trade in fetched_trades:
                            csv_writer.writerow({
                                'timestamp': trade['timestamp'],
                                'price': trade['price'],
                                'amount': trade['amount'],
                                'side': trade['side']
                            })

                    # Save progress
                    progress = {
                        'symbol': symbol,
                        'exchange': exchange.id,
                        'start_date': start_timestamp,
                        'end_date': end_timestamp,
                        'data_type': 'trades',
                        'progress': since
                    }
                    with open(progress_file, 'w') as progress_json:
                        json.dump(progress, progress_json)
                else:
                    since += one_hour
            else:
                since += one_hour
        except ccxt.NetworkError as e:
            print(type(e).__name__, str(e))
            exchange.sleep(60)

    # Remove progress file upon successful completion
    if os.path.isfile(progress_file):
        os.remove(progress_file)
        
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
            
            start_date = pd.to_datetime(start_timestamp, unit='ms').strftime('%Y-%m-%d')
            end_date = pd.to_datetime(end_timestamp, unit='ms').strftime('%Y-%m-%d')

            base_filename = f'{exchange.id}_{symbol.replace("/", "_")}_{timeframe}_ohlcv_{start_date}_{end_date}'
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
            
            start_date = pd.to_datetime(start_timestamp, unit='ms').strftime('%Y-%m-%d')
            end_date = pd.to_datetime(end_timestamp, unit='ms').strftime('%Y-%m-%d')

            orderbook_df['timestamp'] = pd.to_datetime('now')  # Add current timestamp column
            base_filename = f'{exchange.id}_{symbol.replace("/", "_")}_orderbook_{start_date}_{end_date}'
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
            
            start_date = pd.to_datetime(start_timestamp, unit='ms').strftime('%Y-%m-%d')
            end_date = pd.to_datetime(end_timestamp, unit='ms').strftime('%Y-%m-%d')            
            
            ticker_df['timestamp'] = pd.to_datetime('now')  # Add current timestamp column
            base_filename = f'{exchange.id}_{symbol.replace("/", "_")}_ticker_{start_date}_{end_date}'
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
def main(exchange_name, symbol, start_date, end_date, timeframe, market_type, data_type, resume=False):
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
        if market_type == 'perpetuals':
            exchange = ccxt.bitget({
                "apiKey": bitget_api_key,
                "secret": bitget_api_secret,
                "options": {'defaultType': 'swap', 'adjustForTimeDifference': True},
                "timeout": 60000,
                "enableRateLimit": True,
            })
        else:
            exchange = bitget_exchange
    else:
        print(f"Unsupported exchange: {exchange_name}")
        return

    start_timestamp = int(time.mktime(time.strptime(start_date, "%Y-%m-%d"))) * 1000 + 24 * 60 * 60 * 1000
    end_timestamp = int(time.mktime(time.strptime(end_date, "%Y-%m-%d"))) * 1000 + 24 * 60 * 60 * 1000 - 1
    
    print(f"Start Timestamp: {start_timestamp}, End Timestamp: {end_timestamp}")
    
    if data_type == 'ohlcv':
        fetch_ohlcv_and_save(exchange, symbol, start_timestamp, end_timestamp, timeframe)
    elif data_type == 'orderbook':
        fetch_orderbook_and_save(exchange, symbol)
    elif data_type == 'ticker':
        fetch_ticker_and_save(exchange, symbol)
    elif data_type == 'trades':
        fetch_trades_and_save(exchange, symbol, start_timestamp, end_timestamp, resume=resume)
    else:
        print(f"Unsupported data type: {data_type}")
 

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Fetch and save market data')
    parser.add_argument('-exchange', type=str, required=True, help='Exchange name (binance, coinbase, bitget)')
    parser.add_argument('-symbol', type=str, required=True, help='Trading symbol (e.g., BTC/USDT)')
    parser.add_argument('-start_date', type=str, required=True, help='Start date in YYYY-MM-DD format')
    parser.add_argument('-end_date', type=str, required=True, help='End date in YYYY-MM-DD format')
    parser.add_argument('-timeframe', type=str, required=True, help='Timeframe for OHLCV data (e.g., 1m)')
    parser.add_argument('-market_type', type=str, required=True, choices=['spot', 'perpetuals'], help='Market type (spot or perpetuals)')
    parser.add_argument('-data_type', type=str, required=True, choices=['ohlcv', 'orderbook', 'ticker', 'trades'], help='Type of data to fetch (ohlcv, orderbook, ticker, trades)')
    parser.add_argument('-resume', action='store_true', help='Resume fetching data from the last progress')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    main(args.exchange, args.symbol, args.start_date, args.end_date, args.timeframe, args.market_type, args.data_type, args.resume)

    if args.data_type == 'trades':
        if args.exchange == 'binance' and args.market_type == 'perpetuals':
            convert_csv_to_parquet(f'binanceusdm_{args.symbol.replace("/", "_")}_trades_{args.start_date}_{args.end_date}.csv')
        else:
            convert_csv_to_parquet(f'{args.exchange}_{args.symbol.replace("/", "_")}_trades_{args.start_date}_{args.end_date}.csv')
            
    logging.info("Script completed successfully.")
