import os
import backend.config as config
import backend.pipelines.utils as utils
from backend.pipelines.pipeline import PriceDataPipeline
from backend.pipelines.ingestors import YFinanceIngestor,CSVIngestor

# Configuration
yfinance_tickers_ukstock = utils.get_yfinance_tickers("uk stock")
yfinance_tickers_cryptocurrency = utils.get_yfinance_tickers("cryptocurrency")
yfinance_tickers_etf = utils.get_yfinance_tickers("etf")
yfinance_tickers_usstock = utils.get_yfinance_tickers("us stock")
yfinance_batch_size = 100
csv_ticker_source_map = utils.get_csv_ticker_source_map()
csv_base_path = config.EXTERNAL_DATA_BASE_PATH
start_date = "2024-01-01"
end_date = "2025-01-01"
save_path = config.get_price_data_path()

# Instantiate list of ingestors
ingestors = []

# yfinance ingestor
ingestors.append(YFinanceIngestor(yfinance_tickers_usstock,yfinance_batch_size,start_date,end_date))

# # csv ingestors
# for csv in csv_ticker_source_map:
#     ingestors.append(CSVIngestor(csv["ticker"],os.path.join(csv_base_path,csv["source_path"]),start_date,end_date))

# Instantiate and run price pipeline
pipeline = PriceDataPipeline(ingestors,save_path)
pipeline.run()

# Quick test to confirm it works
import polars as pl
prices_df = (pl.scan_parquet(save_path).collect())   
print(prices_df)


