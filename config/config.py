import os

EXTERNAL_DATA_BASE_PATH = "/Volumes/T7/investment_backtester_data"  
PARQUET_PRICE_PATH = "processed/parquet/prices"
PARQUET_DIVIDEND_PATH = "processed/parquet/dividends"
CSV_METAL_PATH = "raw/metals.csv"
METADATA_CSV_NAME = "asset_metadata.csv" 

# this method works provided the file is within the same config folder as the current script
def get_asset_metadata_path():
    return os.path.join(os.path.dirname(__file__),METADATA_CSV_NAME)

def get_price_data_path():
    return os.path.join(EXTERNAL_DATA_BASE_PATH, PARQUET_PRICE_PATH)

def get_metal_data_path():
    return os.path.join(EXTERNAL_DATA_BASE_PATH, CSV_METAL_PATH)

def get_dividend_data_path():
    return os.path.join(EXTERNAL_DATA_BASE_PATH, PARQUET_DIVIDEND_PATH)

