from pathlib import Path

EXTERNAL_DATA_BASE_PATH = Path("/Volumes/T7/investment_backtester_data")  
ASSET_METADATA_PATH = Path("data/asset_metadata.csv")
FX_METADATA_PATH = Path("data/fx_metadata.csv")
PARQUET_BACKTEST_DATA_PATH = Path("compiled/parquet/data")
PARQUET_FX_DATA_PATH = Path("processed/parquet/fx.parquet")
RESULT_BASE_PATH = Path("results")
EXPORT_BASE_PATH = Path("exports")


def get_asset_metadata_path() -> Path:
    return Path(__file__).parent / ASSET_METADATA_PATH

def get_fx_metadata_path() -> Path:
    return Path(__file__).parent / FX_METADATA_PATH

def get_backtest_data_path() -> Path:
    return EXTERNAL_DATA_BASE_PATH / PARQUET_BACKTEST_DATA_PATH

def get_fx_data_path() -> Path:
    return EXTERNAL_DATA_BASE_PATH / PARQUET_FX_DATA_PATH

def get_result_base_path() -> Path:
    return EXTERNAL_DATA_BASE_PATH / RESULT_BASE_PATH

def get_export_base_path() -> Path:
    return EXTERNAL_DATA_BASE_PATH / EXPORT_BASE_PATH 
