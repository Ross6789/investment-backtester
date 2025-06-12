import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import config.config as config
import backend.pipelines.asset_info as asset_info
from backend.pipelines.price_pipeline import PriceDataPipeline

# Configuration
tickers = asset_info.get_metadata("Ticker")
start_date = "2000-01-01"
end_date = "2025-01-01"
save_path = config.get_price_data_path()

# Instantiate and run pipeline
test_pipeline = PriceDataPipeline(tickers,start_date,end_date,save_path)
test_pipeline.run()

# Quick test to confirm it works
import polars as pl
prices_df = (pl.scan_parquet(save_path).collect())   
print(prices_df)



