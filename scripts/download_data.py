from cmds.alpaca_requests import download_intraday
from cmds.data_prep import stack_data
import pandas as pd
import pickle

if __name__ == "__main__":
    import sys

    if (len(sys.argv) < 8) or (len(sys.argv) > 9):
        print("Usage: python download_data.py <SYMBOLS> <FREQ> <START_DATE> <END_DATE> <LIMIT> <API_KEY> <SECRET_KEY> (<STACK>)")
        sys.exit(1)

    if sys.argv[1] in ['-h', '--help']:
        print("Usage: python download_data.py <SYMBOLS> <FREQ> <START_DATE> <END_DATE> <LIMIT> <API_KEY> <SECRET_KEY> (<STACK>)")
        sys.exit(0)
    
    # Set tickers and kwargs
    symbols, freq, start, end, limit, api_key, secret_key = sys.argv[1:8]

    limit = int(limit)
    startDate = pd.to_datetime(start).date()
    endDate = pd.to_datetime(end).date()

    print(f"Downloading intraday data for {symbols.replace(',', ', ')} from {start} to {end} at {freq} resolution...")
    df_dict = download_intraday(symbols, freq, start, end, limit, api_key, secret_key)
    
    # Add df_dict to global namespace
    globals()['df_dict'] = df_dict

    # Immediately pickle downloaded data for reuse
    for symbol, df in df_dict.items():
        with open(f'data/symbols/{symbol}_{startDate}_{endDate}_{freq}.pkl', 'wb') as f:
            pickle.dump(df, f)

        print(f"Saved {symbol} data to data/symbols/{symbol}_{startDate}_{endDate}_{freq}.pkl")

    if len(sys.argv) == 9:
        if sys.argv[6] == 'stack':
            stack_df = stack_data(df_dict)
            print('='*74)
            print('Data aggregared into stacked dataframe.')
            
            globals()['stack_df'] = stack_df