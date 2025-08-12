from pathlib import Path

PROJECT_ROOT_PATH =  Path(__file__).parent.parent.parent
EXTERNAL_DATA_BASE_PATH = Path("/Volumes/T7/investment_backtester_data")  
ASSET_METADATA_CSV_PATH = Path("data/assets.csv")
ASSET_METADATA_JSON_PATH = Path("data/assets.json")
BENCHMARK_METADATA_CSV_PATH = Path("data/benchmarks.csv")
BENCHMARK_METADATA_JSON_PATH = Path("data/benchmarks.json")
FX_METADATA_PATH = Path("data/fx.csv")
PARQUET_BACKTEST_DATA_PATH = Path("compiled/parquet/data")
PARQUET_FX_DATA_PATH = Path("processed/parquet/fx.parquet")
BACKTEST_RUN_BASE_PATH = Path("backtest_runs")

def get_asset_data_csv_path() -> Path:
    return PROJECT_ROOT_PATH / ASSET_METADATA_CSV_PATH

def get_asset_data_json_path() -> Path:
    return PROJECT_ROOT_PATH / ASSET_METADATA_JSON_PATH

def get_benchmark_data_csv_path() -> Path:
    return PROJECT_ROOT_PATH / BENCHMARK_METADATA_CSV_PATH

def get_benchmark_data_json_path() -> Path:
    return PROJECT_ROOT_PATH / BENCHMARK_METADATA_JSON_PATH

def get_fx_data_csv_path() -> Path:
    return PROJECT_ROOT_PATH / FX_METADATA_PATH

def get_backtest_data_path() -> Path:
    return EXTERNAL_DATA_BASE_PATH / PARQUET_BACKTEST_DATA_PATH

def get_fx_data_path() -> Path:
    return EXTERNAL_DATA_BASE_PATH / PARQUET_FX_DATA_PATH

def get_backtest_run_base_path() -> Path:
    return EXTERNAL_DATA_BASE_PATH / BACKTEST_RUN_BASE_PATH

