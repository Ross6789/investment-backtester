import os

DATA_BASE_PATH = "/Volumes/T7/investment_backtester_data"  
METADATA_CSV = "raw/metadata/asset_metadata.csv" 
PARQUET_PRICE_PATH = "raw/prices"
PARQUET_DIVIDEND_PATH = "raw/dividends"

def get_asset_metadata_path():
    return os.path.join(DATA_BASE_PATH, METADATA_CSV)

def get_price_data_path():
    return os.path.join(DATA_BASE_PATH, PARQUET_PRICE_PATH)

def get_dividend_data_path():
    return os.path.join(DATA_BASE_PATH, PARQUET_DIVIDEND_PATH)
