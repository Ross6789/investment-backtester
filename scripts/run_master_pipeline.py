import backend.config as config
import backend.utils as utils
import warnings
from datetime import date
from backend.data_pipeline.pipelines import PricePipeline, CorporateActionPipeline, FXPipeline
from backend.data_pipeline.ingestors import YFinanceIngestor, CSVIngestor
from backend.data_pipeline.runner import PipelineRunner


YELLOW = "\033[93m"
RESET = "\033[0m"

# Configure warning messages
# warnings.simplefilter('always')
# warnings.formatwarning = lambda msg, *args, **kwargs: f"{YELLOW}⚠️ {msg}{RESET}\n"
# warnings.filterwarnings("ignore", message="unclosed database.*") # suppress warnings coming from yfinance, already upgraded to most modern version

# Configuration
yfinance_batch_size = 100
start_date = date.fromisoformat("1970-01-01")
end_date = date.fromisoformat("2025-06-30")
base_path = config.EXTERNAL_DATA_BASE_PATH

# Fetch ticker data
yfinance_test_tickers = ['AAPL','GOOG','MSFT']
yfinance_tickers_ukstock = utils.get_yfinance_tickers("uk stock")
yfinance_tickers_cryptocurrency = utils.get_yfinance_tickers("cryptocurrency")
yfinance_tickers_etf = utils.get_yfinance_tickers("etf")
yfinance_tickers_usstock = utils.get_yfinance_tickers("us stock")
yfinance_tickers_mutualfund = utils.get_yfinance_tickers("mutual fund")
csv_sources = utils.get_asset_csv_sources()
fx_sources = utils.get_fx_csv_sources()
# csv_ticker_source_map = utils.get_csv_ticker_source_map()

# Instantiate list of ingestors
price_ingestors = []
corporate_action_ingestors = []
fx_ingestors = []

# Add price ingestors
price_ingestors.append(YFinanceIngestor(yfinance_test_tickers,yfinance_batch_size,start_date,end_date, include_actions= False))
for relative_source in csv_sources:
    full_source_path = base_path / relative_source
    price_ingestors.append(CSVIngestor(full_source_path,start_date,end_date))

# Add action ingestors
corporate_action_ingestors.append(YFinanceIngestor(yfinance_test_tickers,yfinance_batch_size,start_date,end_date, include_actions=True))

# Add fx ingestors
for relative_source in fx_sources:
    full_source_path = base_path / relative_source
    fx_ingestors.append(CSVIngestor(full_source_path,start_date,end_date))

# Instantiate sub pipelines
price_pipeline = PricePipeline(price_ingestors)
action_pipeline = CorporateActionPipeline(corporate_action_ingestors)
fx_pipeline = FXPipeline(fx_ingestors)

# Instantiate major pipeline and run
master_pipeline = PipelineRunner(price_pipeline,action_pipeline,fx_pipeline,base_path)
master_pipeline.run()