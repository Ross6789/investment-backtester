import sys    
import os  
import yfinance as yf
import polars as pl
import pandas as pd

# Add the root project folder to sys.path to allow config file to be found
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))) 
import config

# Configuration
start_date = "2000-01-01"
end_date = "2025-01-01"

# Retrieve tickers form metadata csv
tickers_df = (pl.scan_csv(config.get_asset_metadata_path()).select("ticker").collect())
tickers = tickers_df["ticker"].to_list()
print(tickers)

# Download price data
data = yf.download(tickers, start = start_date, end = end_date, auto_adjust= False)

for ticker in tickers:

    # Create save path
    price_save_path = os.path.join(config.DATA_BASE_PATH,config.PARQUET_PRICE_PATH,f"{ticker}.parquet")

    # Filter relevant columns using pandas
    df_filtered = data.loc[:, [('Adj Close', ticker), ('Close', ticker)]] 

    # Flatten column names
    df_filtered.columns = ['Adj Close', 'Close']

    # Drop rows with missing price data
    df_filtered = df_filtered.dropna(subset=['Adj Close', 'Close'])

    # Reset index to return date to a regular column
    df_filtered = df_filtered.reset_index()

    # Convert to Polars
    pl_df = pl.from_pandas(df_filtered)

    # Convert datetime to date
    pl_df = pl_df.with_columns(pl.col("Date").cast(pl.Date))

    # Ensure the directory for price_save_path exists
    os.makedirs(os.path.dirname(price_save_path), exist_ok=True)

    # Save data as parquet file
    pl_df.write_parquet(price_save_path)
    
    # Test that it works
    prices_df = (pl.scan_parquet(price_save_path).collect())   
    print(prices_df)
