import backend.core.paths as paths
import backend.utils.metadata as utils
import warnings
from datetime import date
from backend.data_pipeline.pipelines import PricePipeline, CorporateActionPipeline, FXPipeline, BenchmarkPipeline
from backend.data_pipeline.ingestors import YFinanceIngestor, CSVIngestor
from backend.data_pipeline.runner import PipelineRunner


YELLOW = "\033[93m"
RESET = "\033[0m"

# Configure warning messages
warnings.simplefilter('always')
warnings.formatwarning = lambda msg, *args, **kwargs: f"{YELLOW}⚠️ {msg}{RESET}\n"
warnings.filterwarnings("ignore", message="unclosed database.*") # suppress warnings coming from yfinance, already upgraded to most modern version

# Configuration
yfinance_batch_size = 100
start_date = date.fromisoformat("1970-01-01") #Inclusive
end_date = date.fromisoformat("2025-08-01") #Exclusive
backend_data_root_path = paths.BACKEND_DATA_ROOT_PATH

# Fetch ticker data
yfinance_test_tickers = ['AAPL','GOOG','MSFT']
yfinance_tickers_ukstock = utils.get_active_yfinance_tickers("uk stock")
yfinance_tickers_cryptocurrency = utils.get_active_yfinance_tickers("cryptocurrency")
yfinance_tickers_etf = utils.get_active_yfinance_tickers("etf")
yfinance_tickers_usstock = utils.get_active_yfinance_tickers("us stock")
yfinance_tickers_mutualfund = utils.get_active_yfinance_tickers("mutual fund")
asset_csv_sources = utils.get_active_asset_csv_sources()

yfinance_tickers_benchmarks = utils.get_active_yfinance_benchmarks()

fx_csv_sources = utils.get_fx_csv_sources()

# --- INSTANTIATE INGESTOR LISTS
price_ingestors = []
corporate_action_ingestors = []
benchmark_ingestors = []
fx_ingestors = []

# --- TEST INGESTORS (much faster pipeline)
price_ingestors.append(YFinanceIngestor(yfinance_test_tickers,yfinance_batch_size,start_date,end_date,include_actions= False))
corporate_action_ingestors.append(YFinanceIngestor(yfinance_test_tickers,yfinance_batch_size,start_date,end_date, include_actions=True))

# --- PRICE INGESTORS
price_ingestors.append(YFinanceIngestor(yfinance_tickers_ukstock,yfinance_batch_size,start_date,end_date,include_actions= False))
price_ingestors.append(YFinanceIngestor(yfinance_tickers_cryptocurrency,yfinance_batch_size,start_date,end_date,include_actions= False))
price_ingestors.append(YFinanceIngestor(yfinance_tickers_etf,yfinance_batch_size,start_date,end_date,include_actions= False))
price_ingestors.append(YFinanceIngestor(yfinance_tickers_usstock,yfinance_batch_size,start_date,end_date,include_actions= False))
price_ingestors.append(YFinanceIngestor(yfinance_tickers_mutualfund,yfinance_batch_size,start_date,end_date,include_actions= False))
for relative_source in asset_csv_sources:
    full_source_path = backend_data_root_path / relative_source
    price_ingestors.append(CSVIngestor(full_source_path,start_date,end_date))

# --- ACTION INGESTORS
corporate_action_ingestors.append(YFinanceIngestor(yfinance_tickers_ukstock,yfinance_batch_size,start_date,end_date, include_actions=True))
corporate_action_ingestors.append(YFinanceIngestor(yfinance_tickers_etf,yfinance_batch_size,start_date,end_date, include_actions=True))
corporate_action_ingestors.append(YFinanceIngestor(yfinance_tickers_usstock,yfinance_batch_size,start_date,end_date, include_actions=True))
corporate_action_ingestors.append(YFinanceIngestor(yfinance_tickers_mutualfund,yfinance_batch_size,start_date,end_date, include_actions=True))

# --- BENCHMARK INGESTORS
benchmark_ingestors.append(YFinanceIngestor(yfinance_tickers_benchmarks,yfinance_batch_size,start_date,end_date, include_actions=False))

# --- FX INGESTORS
for relative_source in fx_csv_sources:
    full_source_path = backend_data_root_path / relative_source
    fx_ingestors.append(CSVIngestor(full_source_path,start_date,end_date))

# --- INSTANTIATE MINOR PIPELINE
price_pipeline = PricePipeline(price_ingestors)
action_pipeline = CorporateActionPipeline(corporate_action_ingestors)
benchmark_pipeline = BenchmarkPipeline(benchmark_ingestors) 
fx_pipeline = FXPipeline(fx_ingestors)

# --- MASTER PIPELINE 
master_pipeline = PipelineRunner(price_pipeline,action_pipeline,benchmark_pipeline,fx_pipeline, dev_mode=True)
# master_pipeline.run()  # run all pipelines
master_pipeline.run(pipelines_to_run="benchmark")  # run just benchmark pipeline