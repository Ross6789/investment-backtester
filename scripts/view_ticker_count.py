import backend.config as config
import backend.pipelines.utils as utils
import polars as pl

# Configuration
save_path = config.get_price_data_path()

# Quick test to confirm it works and count number of rows for each ticker ie. how much price data
prices_df_eager = pl.read_parquet(save_path)
count_data = prices_df_eager["Ticker"].value_counts().sort(by="count",descending=True)
print(count_data)