# import sys
# import os
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..")))

import polars as pl
from backend import config

# # Retrieve specific column from metadata csv
# def get_metadata(source: str, column: str) -> list[str]:
    
#     # Read the CSV eagerly to inspect and clean column names
#     df = pl.read_csv(config.get_asset_metadata_path())

#     # Clean column names and format as dictionary
#     cleaned_csv_columns = {col: col.strip().lower() for col in df.columns}
#     df = df.rename(cleaned_csv_columns)

#     # Clean arguments 
#     cleaned_source = source.strip().lower()
#     cleaned_column = column.strip().lower()

#     if cleaned_column not in df.columns:
#         raise ValueError(f"Column '{column}' not found in metadata csv.")

#     return (df.filter(pl.col("source")==cleaned_source).get_column(cleaned_column).to_list())
# 
#

def get_yfinance_tickers() -> list[str]:
    metadata = (
        pl.scan_csv(config.get_asset_metadata_path())
        .filter(pl.col("source")=="yfinance")
        .select("ticker")
        .collect()
    )
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