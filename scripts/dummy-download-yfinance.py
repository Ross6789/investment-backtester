from backend.utils.metadata import update_asset_metadata_csv
import yfinance as yf
import polars as pl
from datetime import date
from backend.utils.metadata import get_active_yfinance_tickers
from backend.data_pipeline.pipelines import PricePipeline
from backend.data_pipeline.ingestors import YFinanceIngestor

# actions = yf.download('BTC-GBP','1999-01-01','2025-08-01',auto_adjust=False, actions=True)
# no_actions = yf.download('^STOXX50E','1999-01-01','2025-08-01',auto_adjust=False, actions=False)

# print(actions.info)

# print(no_actions.info)
start = date.fromisoformat('1999-01-01')
end = date.fromisoformat('2025-08-01')

tickers = get_active_yfinance_tickers("cryptocurrency")
crypto = yf.download(tickers,period="max",auto_adjust=False, actions=True)

print(crypto)

pipeline = PricePipeline([YFinanceIngestor(tickers,100,start,end)])
pipeline.run()
print (pipeline.processed_data)