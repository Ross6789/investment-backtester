import polars as pl
from backend.core import paths
from pathlib import Path

def get_yfinance_tickers(asset_type: str) -> list[str]:
    """
    Returns a list of yFinance tickers for the given asset type.

    Args:
        asset_type (str): Type of asset to filter by (e.g., 'stock', 'crypto').

    Returns:
        list[str]: List of matching ticker symbols from yFinance.
    """
    metadata = (
        pl.scan_csv(paths.get_asset_metadata_path())
        .filter((pl.col("source")=="yfinance") & (pl.col("asset_type")==asset_type))
        .select("ticker")
        .collect()
    )
    return metadata["ticker"].to_list()


def get_fx_csv_sources() -> list[Path]:
    """
    Returns a list of all csv source paths for fx data

    Returns:
        list[Path]: List of all csv sources paths within the fx metadata file.
    """
    sources = (
        pl.scan_csv(paths.get_fx_metadata_path())
        .filter(pl.col("source")=="local_csv")
        .select("source_file_path")
        .collect()
        .to_series()
        .to_list()
    )
    return sources


def get_asset_csv_sources() -> list[Path]:
    """
    Returns a list of all csv source paths for asset data

    Returns:
        list[Path]: List of all csv sources paths within the metadata file.
    """
    sources = (
        pl.scan_csv(paths.get_asset_metadata_path())
        .filter(pl.col("source")=="local_csv")
        .select("source_file_path")
        .collect()
        .to_series()
        .to_list()
    )
    return sources


def get_csv_ticker_source_map() -> dict[str, Path]:
    """
    Returns a mapping of tickers to their local CSV file paths.

    Returns:
        dict[str, Path]: Dictionary where keys are ticker symbols and values are local CSV file paths.
    """
    metadata = (
        pl.scan_csv(paths.get_asset_metadata_path())
        .filter(pl.col("source")=="local_csv")
        .select("ticker","source_file_path")
        .collect()
    )
    return {ticker: Path(source_path) for ticker, source_path in metadata.select(["ticker","source_file_path"]).iter_rows()}
