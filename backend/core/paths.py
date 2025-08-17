from pathlib import Path
from typing import Union

# --- ORIGINAL HELPER METHOD STRUCTURE
# PROJECT_ROOT_PATH =  Path(__file__).parent.parent.parent
# EXTERNAL_DATA_BASE_PATH = Path("/Volumes/T7/investment_backtester_data")  
# ASSET_METADATA_CSV_PATH = Path("data/assets.csv")
# ASSET_METADATA_JSON_PATH = Path("data/assets.json")
# BENCHMARK_METADATA_CSV_PATH = Path("data/benchmarks.csv")
# BENCHMARK_METADATA_JSON_PATH = Path("data/benchmarks.json")
# FX_METADATA_PATH = Path("data/fx.csv")
# PARQUET_ASSET_DATA_PATH = Path("compiled/parquet/data")
# PARQUET_BENCHMARK_DATA_PATH = Path("processed/parquet/benchmarks.parquet")
# PARQUET_FX_DATA_PATH = Path("processed/parquet/fx.parquet")
# BACKTEST_RUN_BASE_PATH = Path("backtest_runs")


# def get_asset_metadata_csv_path() -> Path:
#     return PROJECT_ROOT_PATH / ASSET_METADATA_CSV_PATH

# def get_asset_metadata_json_path() -> Path:
#     return PROJECT_ROOT_PATH / ASSET_METADATA_JSON_PATH

# def get_benchmark_metadata_csv_path() -> Path:
#     return PROJECT_ROOT_PATH / BENCHMARK_METADATA_CSV_PATH

# def get_benchmark_metadata_json_path() -> Path:
#     return PROJECT_ROOT_PATH / BENCHMARK_METADATA_JSON_PATH

# def get_fx_metadata_csv_path() -> Path:
#     return PROJECT_ROOT_PATH / FX_METADATA_PATH

# def get_asset_data_path() -> Path:
#     return EXTERNAL_DATA_BASE_PATH / PARQUET_ASSET_DATA_PATH

# def get_benchmark_data_path() -> Path:
#     return EXTERNAL_DATA_BASE_PATH / PARQUET_BENCHMARK_DATA_PATH

# def get_fx_data_path() -> Path:
#     return EXTERNAL_DATA_BASE_PATH / PARQUET_FX_DATA_PATH

# def get_backtest_run_base_path() -> Path:
#     return EXTERNAL_DATA_BASE_PATH / BACKTEST_RUN_BASE_PATH

# --- STRUCTURE AFTER SPLIT INTO A DEV AND PROD ENVIRONMENT
# ---- Base Paths ----
EXTERNAL_DATA_ROOT_PATH = Path("/Volumes/T7/investment_backtester_data")  
BACKEND_DATA_ROOT_PATH = Path(__file__).parent.parent / "data"

METADATA_PATH = BACKEND_DATA_ROOT_PATH / "metadata"
BACKTEST_PATH = BACKEND_DATA_ROOT_PATH / "backtests"

DEV_INPUTS = EXTERNAL_DATA_ROOT_PATH / "inputs" / "dev"
PROD_INPUTS = BACKTEST_PATH / "inputs" / "prod"

# Public URLs for Google Cloud Production Data
PROD_HISTORICAL_PRICES_URL = "https://storage.googleapis.com/qub-40286439-backtester-data/historical-prices.parquet"
PROD_BENCHMARKS_URL = "https://storage.googleapis.com/qub-40286439-backtester-data/benchmarks.parquet"
PROD_FX_URL = "https://storage.googleapis.com/qub-40286439-backtester-data/fx-rates.parquet"

# Paths for local development data (external drive)
DEV_HISTORICAL_PRICES_PATH = DEV_INPUTS / "historical-prices.parquet"  # Previously had no .parquet extension due to partitioned structure, but small enough to cache on server when starting so no longer need partition
DEV_BENCHMARKS_PATH = DEV_INPUTS / "benchmarks.parquet"
DEV_FX_PATH = DEV_INPUTS / "fx-rates.parquet"

# --- Ingestion helper
def get_data_ingestion_path(dev_mode: bool = False) -> Path:
    return DEV_INPUTS if dev_mode else PROD_INPUTS

# ---- Metadata Helpers ----
def get_asset_metadata_csv_path() -> Path:
    return METADATA_PATH / "csv" / "assets.csv"

def get_asset_metadata_json_path(dev_mode: bool = False) -> Path:
    folder = "dev" if dev_mode else "prod"
    return METADATA_PATH / "json" / folder / "assets.json"

def get_benchmark_metadata_csv_path() -> Path:
    return METADATA_PATH / "csv" / "benchmarks.csv"

def get_benchmark_metadata_json_path(dev_mode: bool = False) -> Path:
    folder = "dev" if dev_mode else "prod"
    return METADATA_PATH / "json" / folder / "benchmarks.json"

def get_fx_metadata_csv_path() -> Path:
    return METADATA_PATH / "csv" / "fx.csv"

# ---- Data source Helpers ----
def get_historical_prices_data_source(dev_mode: bool = False) -> Union[Path, str]:
    return DEV_HISTORICAL_PRICES_PATH if dev_mode else PROD_HISTORICAL_PRICES_URL

def get_benchmark_data_source(dev_mode: bool = False) -> Union[Path, str]:
    return DEV_BENCHMARKS_PATH if dev_mode else PROD_BENCHMARKS_URL

def get_fx_data_source(dev_mode: bool = False) -> Union[Path,str]:
    return DEV_FX_PATH if dev_mode else PROD_FX_URL

# ---- Backtest Results ----
def get_backtest_run_base_path() -> Path:
    return BACKTEST_PATH / "results"



# class DataPaths:
#     def __init__(self, dev_mode: bool = False):
#         self.dev_mode = dev_mode

#         # Base roots
#         self.backend_root = Path(__file__).parent.parent
#         self.data_root = self.backend_root / "data"
#         self.external_base = Path("/Volumes/T7/investment_backtester_data")

#         # Sub-folders
#         self.metadata_root = self.data_root / "metadata"
#         self.backtests_root = self.data_root / "backtests"
#         self.results_root = self.backtests_root / "results"

#         self.dev_input = self.backtests_root / "inputs" / "dev"
#         self.prod_input = self.backtests_root / "inputs" / "prod"

#     # ---- Metadata ----
#     def csv_metadata(self, file_name: str) -> Path:
#         return self.metadata_root / "csv" / f"{file_name}.csv"

#     def json_metadata(self, file_name: str) -> Path:
#         return self.metadata_root / "json" / f"{file_name}.json"

#     # ---- Input data ----
#     @property
#     def backtest_input(self) -> Path:
#         return self.dev_input if self.dev_mode else self.prod_input

#     @property
#     def asset_parquet(self) -> Path:
#         return self.backtest_input / ("historical-prices.parquet" if self.dev_mode else "historical-prices")

#     @property
#     def benchmark_parquet(self) -> Path:
#         return self.backtest_input / "benchmarks.parquet"

#     @property
#     def fx_parquet(self) -> Path:
#         return self.backtest_input / "fx-rates.parquet"

#     # ---- Results ----
#     @property
#     def backtest_results(self) -> Path:
#         return self.results_root
