import os
import polars as pl
import backend.config as config
import backend.pipelines.utils as utils
from backend.pipelines.pipeline import DataPipeline
from backend.pipelines.ingestors import YFinancePriceIngestor,CSVPriceIngestor, YFinanceCorporateActionsIngestor

# Configuration
yfinance_batch_size = 100
start_date = "1970-01-01"
end_date = "2025-06-01"
csv_base_path = config.EXTERNAL_DATA_BASE_PATH
csv_ticker_source_map = utils.get_csv_ticker_source_map()
parquet_price_save_path = config.get_parquet_price_path()
parquet_corporate_actions_save_path = config.get_parquet_corporate_action_path()
csv_price_save_path = config.get_csv_price_path()
csv_corporate_actions_save_path = config.get_csv_corporate_action_path()

# Ticker lists
yfinance_tickers_ukstock = utils.get_yfinance_tickers("uk stock")
yfinance_tickers_cryptocurrency = utils.get_yfinance_tickers("cryptocurrency")
yfinance_tickers_etf = utils.get_yfinance_tickers("etf")
yfinance_tickers_usstock = utils.get_yfinance_tickers("us stock")
yfinance_tickers_mutualfund = utils.get_yfinance_tickers("mutual fund")

# Instantiate list of ingestors
price_ingestors = []
corporate_action_ingestors = []

# yfinance price ingestors
price_ingestors.append(YFinancePriceIngestor(yfinance_tickers_ukstock,yfinance_batch_size,start_date,end_date))
price_ingestors.append(YFinancePriceIngestor(yfinance_tickers_cryptocurrency,yfinance_batch_size,start_date,end_date))
price_ingestors.append(YFinancePriceIngestor(yfinance_tickers_etf,yfinance_batch_size,start_date,end_date))
price_ingestors.append(YFinancePriceIngestor(yfinance_tickers_usstock,yfinance_batch_size,start_date,end_date))
price_ingestors.append(YFinancePriceIngestor(yfinance_tickers_mutualfund,yfinance_batch_size,start_date,end_date))

# csv price ingestors
for csv in csv_ticker_source_map:
    price_ingestors.append(CSVPriceIngestor(csv["ticker"],os.path.join(csv_base_path,csv["source_path"]),start_date,end_date))

# yfinance corporate action ingestors
corporate_action_ingestors.append(YFinanceCorporateActionsIngestor(yfinance_tickers_ukstock,yfinance_batch_size,start_date,end_date))
corporate_action_ingestors.append(YFinanceCorporateActionsIngestor(yfinance_tickers_cryptocurrency,yfinance_batch_size,start_date,end_date))
corporate_action_ingestors.append(YFinanceCorporateActionsIngestor(yfinance_tickers_etf,yfinance_batch_size,start_date,end_date))
corporate_action_ingestors.append(YFinanceCorporateActionsIngestor(yfinance_tickers_usstock,yfinance_batch_size,start_date,end_date))
corporate_action_ingestors.append(YFinanceCorporateActionsIngestor(yfinance_tickers_mutualfund,yfinance_batch_size,start_date,end_date))

# # Instantiate and run price pipeline
# price_pipeline = DataPipeline(price_ingestors,price_save_path)
# price_pipeline.run()

# Instantiate and run price pipeline
corporate_action_pipeline = DataPipeline(corporate_action_ingestors,parquet_corporate_actions_save_path)
corporate_action_pipeline.run()

# Print to screen and csv to allow human readable check
prices_df = (pl.scan_parquet(parquet_price_save_path).collect())   
prices_df.write_csv(csv_price_save_path)   
print(prices_df)

actions_df = (pl.scan_parquet(parquet_corporate_actions_save_path).collect())
actions_df.write_csv(csv_corporate_actions_save_path)   
print(actions_df)