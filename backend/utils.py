import polars as pl
from backend import config
from datetime import datetime, date

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

def parse_date(date_str: str) -> date:
    """
    Converts a date string in 'YYYY-MM-DD' format to a datetime.date object.

    Args:
        date_str (str): Date string, e.g. '2024-01-01'

    Returns:
        datetime.date: Parsed date object

    Raises:
        ValueError: If the input string is not in the expected format.
    """
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError as e:
        raise ValueError(f"Invalid date format: '{date_str}'. Expected 'YYYY-MM-DD'.") from e