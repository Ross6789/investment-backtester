from pathlib import Path

BACKEND_ROOT_PATH =  Path(__file__).parent.parent
EXTERNAL_DATA_BASE_PATH = Path("/Volumes/T7/investment_backtester_data")  
ASSET_METADATA_PATH = Path("data/asset_metadata.csv")
FX_METADATA_PATH = Path("data/fx_metadata.csv")
PARQUET_BACKTEST_DATA_PATH = Path("compiled/parquet/data")
PARQUET_FX_DATA_PATH = Path("processed/parquet/fx.parquet")
BACKTEST_RUN_BASE_PATH = Path("backtest_runs")

def get_asset_metadata_path() -> Path:
    return BACKEND_ROOT_PATH / ASSET_METADATA_PATH

def get_fx_metadata_path() -> Path:
    return BACKEND_ROOT_PATH / FX_METADATA_PATH

def get_backtest_data_path() -> Path:
    return EXTERNAL_DATA_BASE_PATH / PARQUET_BACKTEST_DATA_PATH

def get_fx_data_path() -> Path:
    return EXTERNAL_DATA_BASE_PATH / PARQUET_FX_DATA_PATH

def get_backtest_run_base_path() -> Path:
    return EXTERNAL_DATA_BASE_PATH / BACKTEST_RUN_BASE_PATH

