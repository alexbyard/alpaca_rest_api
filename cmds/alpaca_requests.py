import requests
import pandas as pd
import numpy as np
import datetime as dt 
from datetime import time 

def safe_get(url, headers, params):
    '''
    Sends a GET request to the specified Alpaca REST API endpoint with automatic rate-limit handling.

    If the server responds with HTTP 429 (Too Many Requests), the function reads the 
    'X-RateLimit-Reset' response header to determine how long to wait before retrying.
    It sleeps for the required duration and retries the same request until a successful 
    response is received.

    Args:
        url (str): The API endpoint URL.
        headers (dict): HTTP headers, including Alpaca API authentication.
        params (dict): Query parameters for the GET request.

    Returns:
        requests.Response: The HTTP response object from the successful request.
    '''
    while True:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 429:
            reset_ts = int(response.headers.get('X-RateLimit-Reset', time.time() + 60))
            sleep_seconds = max(reset_ts - int(time.time()), 1)
            print(f"Rate limit exceeded. Sleeping for {sleep_seconds} seconds...")
            time.sleep(sleep_seconds)
        else:
            return response

def download_intraday_dict(symbols: str, timeframe: str, start: str, end: str, limit: int, api_key: str, secret_key: str, stocks: bool = True):
    """
    Fetches raw intraday bar data for specified symbols from the Alpaca API.

    This function retrieves bar data for the given symbols
    within the specified time range and timeframe. It handles pagination and rate limiting.

    Parameters:
        symbols (str): Comma-separated list of stock symbols (e.g., 'AAPL,TSLA').
        timeframe (str): Timeframe for the bars (e.g., '1Min', '5Min').
        start (str): ISO 8601 formatted start datetime (e.g., '2022-01-03T09:30:00-04:00').
        end (str): ISO 8601 formatted end datetime (e.g., '2022-01-04T09:30:00-04:00').
        limit (int): Maximum number of bars per request (must be <= 10000).
        api_key (str): Alpaca API key.
        secret_key (str): Alpaca secret key.
        stocks (bool, optional): If True, fetches stock data; currently only supports stocks.

    Returns:
        dict: A dictionary where keys are symbol strings and values are lists of bar data dictionaries.
    """

    if limit > 10000:
        raise ValueError('limit must be <= 10000 (Alpaca API restriction)')
    
    API_KEY = api_key
    SECRET_KEY = secret_key

    if stocks:
        BASE_URL = 'https://data.alpaca.markets/v2/stocks/bars'

    headers = {
        'accept': 'application/json',
        'APCA-API-KEY-ID': API_KEY,
        'APCA-API-SECRET-KEY': SECRET_KEY
    }

    symbol_bars = {}
    next_token = None
    while True:
        params = {
            'symbols': symbols,
            'timeframe': timeframe,
            'start': start,
            'end': end,
            'adjustment': 'raw',
            'feed': 'sip',
            'sort': 'asc',
            'limit': limit
        }
        if next_token:
            params['page_token'] = next_token

        response = safe_get(BASE_URL, headers, params)
        data = response.json()

        for symbol, barlist in data.get("bars", {}).items():
            symbol_bars.setdefault(symbol, []).extend(barlist)

            if barlist:
                first_time = barlist[0]['t']
                last_time = barlist[-1]['t']
                print(f"[{symbol}] Fetched {len(barlist)} bars from {first_time} to {last_time}")

        next_token = data.get("next_page_token")
        if not next_token:
            break

    return symbol_bars

def download_intraday(symbols: str, timeframe: str, start: str, end: str, limit: int, api_key: str, secret_key: str, stocks: bool = True):
    """
    Retrieves and processes intraday bar data for specified symbols from the Alpaca API.

    This function fetches raw 5 minute bar data using `download_intraday_dict`, processes it into pandas DataFrames,
    filters out premarket and after-hours data (keeping only 9:30 AM to 3:55 PM US/Eastern),
    removes incomplete trading days, and eliminates duplicate timestamps.

    Parameters:
        symbols (str): Comma-separated list of stock symbols (e.g., 'AAPL,TSLA').
        timeframe (str): Timeframe for the bars (e.g., '1Min', '5Min', only works for '5Min').
        start (str): ISO 8601 formatted start datetime (e.g., '2022-01-03T09:30:00-04:00').
        end (str): ISO 8601 formatted end datetime (e.g., '2023-01-04T09:30:00-04:00').
        limit (int): Maximum number of bars per request (must be <= 10000).
        api_key (str): Alpaca API key.
        secret_key (str): Alpaca secret key.
        stocks (bool, optional): If True, fetches stock data; currently only supports stocks.

    Returns:
        dict: A dictionary where keys are symbol strings and values are pandas DataFrames
              containing processed intraday bar data with datetime indices.
    """
    
    symbol_bars = download_intraday_dict(symbols, timeframe, start, end, limit, api_key, secret_key)

    df_dict = {}

    for symbol, _ in symbol_bars.items():
        temp_df = pd.DataFrame(symbol_bars[symbol])

        # Rename columns
        temp_df.rename(columns={'c': 'close', 'h': 'high', 'l': 'low', 'n': 'count', 'o': 'open', 't': 'datetime', 'v': 'volume', 'vw': 'vwap'}, inplace=True)

        # Get datetime in ET & remove premarket and after hours data
        temp_df['datetime'] = pd.to_datetime(temp_df['datetime'], utc=True).dt.tz_convert('US/Eastern')
        temp_df = temp_df[temp_df['datetime'].dt.time.between(time(9, 30), time(15, 55))]

        # Set datetime as index
        temp_df.index = temp_df['datetime']
        temp_df = temp_df.drop(columns='datetime')

        # Reorder columns
        temp_df = temp_df[['open', 'high', 'low', 'close', 'volume', 'count', 'vwap']]
        
        temp_date_range = np.unique(temp_df.index.date)
        temp_com_day_list = []
        com_day_count = 0
        first = True
        skipped_day = False
        for date in temp_date_range:
            temp_day_df = temp_df[temp_df.index.date == date]

            if len(temp_day_df) >= 78: # Change for anything other than 5m data
                temp_com_day_list.append(temp_day_df)
                com_day_count += 1
            else:
                if first:
                    print('='*74)
                    first = False
                skipped_day = True
                print(f'[{symbol}] Date {date} is incomplete with only {len(temp_day_df)} bars. Removing data.')
        
        if first:
            print('='*74)

        temp_df = pd.concat(temp_com_day_list, axis=0)

        dupes = temp_df.index.duplicated().sum()
        if dupes > 0:
            print(f"[{symbol}] Warning: {dupes} duplicate bars found. Removing duplicates.")
        temp_df = temp_df[~temp_df.index.duplicated(keep='first')]

        if skipped_day:
            print('-'*70)
        print(f"Downloaded {symbol} {timeframe} data from {temp_df.index[0]} to {temp_df.index[-1]} across {com_day_count} complete days.")

        temp_df['symbol'] = symbol

        df_dict[symbol] = temp_df

    print('='*74)

    return df_dict