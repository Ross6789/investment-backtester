import sys    
import os  
import yfinance as yf
import polars as pl

# Add the root project folder to sys.path to allow config file to be found
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))) 
import config

# Configuration
ticker = "AAPL"
start_date = "2024-12-01"
end_date = "2024-12-31"
price_save_path = os.path.join(config.DATA_BASE_PATH,config.PARQUET_PRICE_PATH,"AAPL.parquet")

# Download price data
data = yf.download(ticker, start = start_date, end = end_date, auto_adjust= False)

# Filter relevant columns using pandas
df_filtered = data.loc[:, [('Adj Close', 'AAPL'), ('Close', 'AAPL')]]

# Flatten column names
df_filtered.columns = ['Adj Close', 'Close']

# Reset index to return date to a regular column
df_filtered = df_filtered.reset_index()

# Convert to Polars
pl_df = pl.from_pandas(df_filtered)

# Convert datetime to date
pl_df = pl_df.with_columns(pl.col("Date").cast(pl.Date))

print(pl_df.head(10))

# Ensure the directory for price_save_path exists
os.makedirs(os.path.dirname(price_save_path), exist_ok=True)

# Save data as parquet file
pl_df.write_parquet(price_save_path)
