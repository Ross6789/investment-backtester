import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import backend.config as config
import backend.pipelines.utils as utils
from backend.pipelines.ingestors import PriceDataPipeline

# Configuration
tickers = utils.get_metadata("Ticker")
start_date = "2000-01-01"
end_date = "2025-01-01"
save_path = config.get_price_data_path()

# # Quick test to confirm it works and count number of rows for each ticker ie. how much price data
import polars as pl
prices_df_eager = pl.read_parquet(save_path)
count_data = prices_df_eager["Ticker"].value_counts().sort(by="count",descending=True)
print(count_data)