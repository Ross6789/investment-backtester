import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import config.config as config
import backend.pipelines.asset_info as asset_info
from backend.pipelines.price_pipeline import PriceDataPipeline
from backend.pipelines.metal_pipeline import MetalDataPipeline

# Configuration
tickers = asset_info.get_metadata("Ticker")
metal_tickers = ["GC=F","SI=F","PL=F","GBPUSD=X"]
start_date = "1900-01-01"
end_date = "2025-01-01"
price_save_path = config.get_price_data_path()
metal_save_path = config.get_metal_data_path()

# # Instantiate and run price pipeline
# test_pipeline = PriceDataPipeline(tickers,start_date,end_date,save_path)
# test_pipeline.run()

# Instantiate and run metal pipeline
test_pipeline = MetalDataPipeline(metal_tickers,start_date,end_date,metal_save_path)
test_pipeline.run()

# Quick test to confirm it works
# import polars as pl
# prices_df = (pl.scan_parquet(save_path).collect())   
# print(prices_df)



