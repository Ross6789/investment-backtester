import os
from datetime import datetime

EXTERNAL_DATA_BASE_PATH = "/Volumes/T7/investment_backtester_data"  
METADATA_PATH = "data/asset_metadata.csv" 
PARQUET_PRICE_BASE_PATH = "processed/parquet/prices"
PARQUET_CORPORATE_ACTIONS_BASE_PATH = "processed/parquet/corporate_actions"
CSV_PRICE_PATH = "processed/csv/prices.csv"
CSV_CORPORATE_ACTIONS_PATH = "processed/csv/corporate_actions.csv"
CSV_HAS_CORPORATE_ACTIONS_PATH = "processed/csv/count_corporate_actions.csv"
CSV_BACKTEST_RESULT_PATH = "results/csv"
CSV_METAL_PATH = "raw/metals.csv"


# this method works provided the metadata file path is relative to the folder containing this config file
def get_asset_metadata_path():
    return os.path.join(os.path.dirname(__file__),METADATA_PATH)

def get_parquet_price_base_path():
    return os.path.join(EXTERNAL_DATA_BASE_PATH, PARQUET_PRICE_BASE_PATH)

def get_parquet_corporate_action_base_path():
    return os.path.join(EXTERNAL_DATA_BASE_PATH, PARQUET_CORPORATE_ACTIONS_BASE_PATH)

def get_csv_price_path():
    return os.path.join(EXTERNAL_DATA_BASE_PATH, CSV_PRICE_PATH)

def get_csv_corporate_action_path():
    return os.path.join(EXTERNAL_DATA_BASE_PATH, CSV_CORPORATE_ACTIONS_PATH)

def get_csv_has_corporate_action_path():
    return os.path.join(EXTERNAL_DATA_BASE_PATH, CSV_HAS_CORPORATE_ACTIONS_PATH)

def get_csv_backtest_result_path():
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'backtest_{timestamp}.csv'
    return os.path.join(EXTERNAL_DATA_BASE_PATH, CSV_BACKTEST_RESULT_PATH, filename)

def get_metal_data_path():
    return os.path.join(EXTERNAL_DATA_BASE_PATH, CSV_METAL_PATH)