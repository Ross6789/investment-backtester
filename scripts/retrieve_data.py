import yfinance as yf
import pandas as pd
import polars as pl

# Configuration
tickers = ["AAPL","GOOG"]
start_date = "2024-12-01"
end_date = "2024-12-31"

# Download price data
data = yf.download(tickers, start = start_date, end = end_date, auto_adjust= False)

# Filter relevant columns using pandas
df_filtered = data.loc[:, [('Adj Close', 'AAPL'), ('Close', 'AAPL')]]

# Flatten column names
df_filtered.columns = ['Adj Close', 'Close']

# Reset index to return date to a regular column
df_filtered = df_filtered.reset_index()

# Convert to Polars
pl_df = pl.from_pandas(df_filtered)

print(pl_df.head(10))

