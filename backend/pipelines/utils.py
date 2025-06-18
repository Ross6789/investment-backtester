import polars as pl
from backend import config

def get_yfinance_tickers(asset_type: str) -> list[str]:
    metadata = (
        pl.scan_csv(config.get_asset_metadata_path())
        .filter((pl.col("source")=="yfinance") & (pl.col("asset_type")==asset_type))
        .select("ticker")
        .collect()
    )
    print(f"{asset_type} ticker subset :")
    print(metadata.head())
    return metadata["ticker"].to_list()

def get_csv_ticker_source_map() -> dict[str: str]:
    metadata = (
        pl.scan_csv(config.get_asset_metadata_path())
        .filter(pl.col("source")=="local_csv")
        .select("ticker","source_file_path")
        .collect()
    )
    return [{"ticker": ticker, "source_path": source_path} for ticker, source_path in metadata.select(["ticker","source_file_path"]).iter_rows()]
    
# yfinance_tickers = get_yfinance_tickers()    
# print("YFinance tickers : ")
# for ticker in yfinance_tickers:
#     print(ticker)

# csv_tickers = get_csv_ticker_source_map()
# print("CSV ticker dicts : ")
# for ticker in csv_tickers:
#     print(ticker)