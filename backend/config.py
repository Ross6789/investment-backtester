from pathlib import Path
from datetime import datetime

EXTERNAL_DATA_BASE_PATH = Path("/Volumes/T7/investment_backtester_data")  
METADATA_PATH = Path("data/asset_metadata.csv")
PARQUET_BACKTEST_DATA_PATH = Path("compiled/parquet/data")
CSV_BACKTEST_RESULT_PATH = Path("results/csv")


def get_asset_metadata_path() -> Path:
    return Path(__file__).parent / METADATA_PATH

def get_backtest_data_path() -> Path:
    return EXTERNAL_DATA_BASE_PATH / PARQUET_BACKTEST_DATA_PATH

def get_csv_backtest_result_path() -> Path:
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    return EXTERNAL_DATA_BASE_PATH / CSV_BACKTEST_RESULT_PATH / f'backtest_{timestamp}.csv'
