import os
import backend.config as config
import backend.pipelines.utils as utils
from backend.pipelines.pipeline import PriceDataPipeline
from backend.pipelines.ingestors import YFinanceIngestor,CSVIngestor

# Read metadata file


# Configuration
yfinance_tickers = utils.get_yfinance_tickers()
yfinance_batch_size = 100
csv_ticker_source_map = utils.get_csv_ticker_source_map()
csv_base_path = config.EXTERNAL_DATA_BASE_PATH
start_date = "2024-01-01"
end_date = "2025-01-01"
save_path = config.get_price_data_path()

# Instantiate list of ingestors
ingestors = []

# # yfinance ingestor
# ingestors.append(YFinanceIngestor(yfinance_tickers,yfinance_batch_size,start_date,end_date))

# csv ingestors
for csv in csv_ticker_source_map:
    ingestors.append(CSVIngestor(csv["ticker"],os.path.join(csv_base_path,csv["source_path"]),start_date,end_date))

# Instantiate and run price pipeline
pipeline = PriceDataPipeline(ingestors,save_path)
pipeline.run()

# Quick test to confirm it works
import polars as pl
prices_df = (pl.scan_parquet(save_path).collect())   
print(prices_df)

# import sys
# import os
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# import config.config as config
# import backend.pipelines.asset_info as asset_info
# from backend.pipelines.ingestors import PriceDataPipeline
# from backend.pipelines.metal_pipeline import MetalDataPipeline

# # Configuration
# tickers = asset_info.get_metadata("us stock","Ticker")
# metal_tickers = ["GC=F","SI=F","PL=F","GBPUSD=X"]
# start_date = "1900-01-01"
# end_date = "2025-01-01"
# price_save_path = config.get_price_data_path()
# metal_save_path = config.get_metal_data_path()

# # Instantiate and run price pipeline
# test_pipeline = PriceDataPipeline(tickers,start_date,end_date,save_path)
# test_pipeline.run()

# # Instantiate and run metal pipeline
# test_pipeline = MetalDataPipeline(metal_tickers,start_date,end_date,metal_save_path)
# test_pipeline.run()

# # Quick test to confirm it works
# import polars as pl
# prices_df = (pl.scan_parquet(price_save_path).collect())   
# print(prices_df)



