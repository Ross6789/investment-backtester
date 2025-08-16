import backend.core.paths as paths
import backend.utils as utils
import polars as pl
import pandas as pd
from datetime import date
from backend.data_pipeline.ingestors import YFinanceIngestor
from backend.data_pipeline.pipelines import PricePipeline, CorporateActionPipeline
from backend.data_pipeline.compiler import Compiler

# Configuration
save_path = paths.get_production_historical_prices_path()
metadata_path = paths.get_asset_metadata_csv_path()
fx_path = paths.get_fx_data_path()

# Quick test to confirm it works and count number of rows for each ticker ie. how much price data
prices = pl.read_parquet(save_path)
metadata = pl.read_csv(metadata_path)
fx = pl.read_parquet(fx_path)

prices_tickers = prices.select('ticker')
metadata_tickers = metadata.select('ticker')
no_valid_price = metadata_tickers.join(prices_tickers, on='ticker',how='anti')

print(f'Missing prices : {no_valid_price.to_series().to_list()}')
print(f'All tickers have price : {set(prices_tickers.to_series().to_list()) == set (metadata_tickers.to_series().to_list())}')

print(prices)
count_data = prices["ticker"].value_counts().sort(by="count",descending=True)
print(count_data)
print(fx)

# # Code to update crypto currency to overcome limited data bug
# yfinance_batch_size = 100
# start_date = date.fromisoformat("2000-01-01")
# end_date = date.fromisoformat("2025-06-30")
# base_path = config.EXTERNAL_DATA_BASE_PATH
# yfinance_tickers_cryptocurrency = utils.get_yfinance_tickers("cryptocurrency")

# price_ingestor = YFinanceIngestor(yfinance_tickers_cryptocurrency,yfinance_batch_size,start_date,end_date,include_actions= False)
# price_pipeline = PricePipeline([price_ingestor])
# price_pipeline.run()

# action_data = pl.read_csv(config.EXTERNAL_DATA_BASE_PATH / 'processed/csv/corporate_actions.csv',try_parse_dates=True)

# compiled_data = Compiler.compile(price_pipeline.processed_data, action_data)

# parquet_path = config.EXTERNAL_DATA_BASE_PATH / 'compiled/parquet/data'
# utils.save_partitioned_parquet(compiled_data,parquet_path)