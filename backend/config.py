from pathlib import Path
from datetime import datetime

EXTERNAL_DATA_BASE_PATH = Path("/Volumes/T7/investment_backtester_data")  
METADATA_PATH = Path("data/asset_metadata.csv")
PARQUET_PRICE_BASE_PATH = Path("processed/parquet/prices")
PARQUET_CORPORATE_ACTIONS_BASE_PATH = Path("processed/parquet/corporate_actions")
CSV_PRICE_PATH = Path("processed/csv/prices.csv")
CSV_CORPORATE_ACTIONS_PATH = Path("processed/csv/corporate_actions.csv")
CSV_HAS_CORPORATE_ACTIONS_PATH = Path("processed/csv/count_corporate_actions.csv")
CSV_BACKTEST_RESULT_PATH = Path("results/csv")
CSV_METAL_PATH = Path("raw/metals.csv")


def get_asset_metadata_path() -> Path:
    return Path(__file__).parent / METADATA_PATH

def get_parquet_price_base_path() -> Path:
    return EXTERNAL_DATA_BASE_PATH / PARQUET_PRICE_BASE_PATH

def get_parquet_corporate_action_base_path() -> Path:
    return EXTERNAL_DATA_BASE_PATH / PARQUET_CORPORATE_ACTIONS_BASE_PATH

def get_csv_price_path() -> Path:
    return EXTERNAL_DATA_BASE_PATH / CSV_PRICE_PATH

def get_csv_corporate_action_path() -> Path:
    return EXTERNAL_DATA_BASE_PATH / CSV_CORPORATE_ACTIONS_PATH

def get_csv_has_corporate_action_path() -> Path:
    return EXTERNAL_DATA_BASE_PATH / CSV_HAS_CORPORATE_ACTIONS_PATH

def get_csv_backtest_result_path() -> Path:
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    return EXTERNAL_DATA_BASE_PATH / CSV_BACKTEST_RESULT_PATH / f'backtest_{timestamp}.csv'

def get_metal_data_path() -> Path:
    return EXTERNAL_DATA_BASE_PATH / CSV_METAL_PATH