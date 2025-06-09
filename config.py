DATA_BASE_PATH = "/T7/investment_backtester_data"  
METADATA_CSV = "raw/metadata/asset_metadata.csv" 

def get_asset_metadata_path():
    import os
    return os.path.join(DATA_BASE_PATH, METADATA_CSV)