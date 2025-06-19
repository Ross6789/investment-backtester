import os

EXTERNAL_DATA_BASE_PATH = "/Volumes/T7/investment_backtester_data"  
METADATA_PATH = "data/asset_metadata.csv" 
PARQUET_PRICE_PATH = "processed/parquet/prices"
PARQUET_CORPORATE_ACTIONS_PATH = "processed/parquet/corporate_actions"
CSV_METAL_PATH = "raw/metals.csv"


# this method works provided the metadata file path is relative to the folder containing this config file
def get_asset_metadata_path():
    return os.path.join(os.path.dirname(__file__),METADATA_PATH)

def get_price_data_path():
    return os.path.join(EXTERNAL_DATA_BASE_PATH, PARQUET_PRICE_PATH)

def get_corporate_action_data_path():
    return os.path.join(EXTERNAL_DATA_BASE_PATH, PARQUET_CORPORATE_ACTIONS_PATH)

def get_metal_data_path():
    return os.path.join(EXTERNAL_DATA_BASE_PATH, CSV_METAL_PATH)