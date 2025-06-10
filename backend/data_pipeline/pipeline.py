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

# Orignal : Retrieve tickers form metadata csv
tickers_df = (pl.scan_csv(config.get_asset_metadata_path()).select("ticker").collect())
tickers = tickers_df["ticker"].to_list()
print(tickers)

# Retrieve tickers method
def get_tickers(path, column):
    tickers_df = (pl.scan_csv(path).select(column).collect())
    return tickers_df["ticker"].to_list()

# Retrieve tickers form metadata csv
tickers = get_tickers(config.get_asset_metadata_path(),"ticker")
print(tickers)

# Download price data
data = yf.download(tickers, start = start_date, end = end_date, auto_adjust= False)

# Create an empty list to hold dataframes
dfs = []

for ticker in tickers:

    # Filter relevant columns using pandas
    df_filtered = data.loc[:, [('Adj Close', ticker), ('Close', ticker)]] 

    # Flatten column names
    df_filtered.columns = ['Adj Close', 'Close']

    # Drop rows with missing price data
    df_filtered = df_filtered.dropna(subset=['Adj Close', 'Close'])

    # Reset index to return date to a regular column
    df_filtered = df_filtered.reset_index()

    # Add ticker column
    df_filtered['Ticker'] = ticker

    # Add df to list
    dfs.append(df_filtered)

# Concatenate all tickers into a single pandas DataFrame
all_data_pd = pd.concat(dfs, ignore_index=True)

# Convert to Polars
all_data_pl = pl.from_pandas(all_data_pd)

# Convert datetime to date
all_data_pl = all_data_pl.with_columns(pl.col("Date").cast(pl.Date))

# Create save folder
partitioned_path = os.path.join(config.DATA_BASE_PATH,config.PARQUET_PRICE_PATH)

# Save data as parquet file
all_data_pl.write_parquet(
    partitioned_path,
    use_pyarrow=True,
    partition_by=["Ticker"]
)
    
# Test that it works
prices_df = (pl.scan_parquet(partitioned_path).collect())   
print(prices_df)