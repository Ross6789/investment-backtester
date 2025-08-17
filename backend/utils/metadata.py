import polars as pl
from backend.core import paths
from pathlib import Path
import csv, json
from datetime import date

def get_active_yfinance_tickers(asset_type: str) -> list[str]:
    """
    Returns a list of yFinance tickers for the given asset type.

    Args:
        asset_type (str): Type of asset to filter by (e.g., 'stock', 'crypto').

    Returns:
        list[str]: List of matching ticker symbols from yFinance.
    """
    metadata = (
        pl.scan_csv(paths.get_asset_metadata_csv_path())
        .filter((pl.col("active")=="Y")&(pl.col("source")=="yfinance") & (pl.col("asset_type")==asset_type))
        .select("ticker")
        .collect()
    )
    return metadata["ticker"].to_list()


def get_active_yfinance_benchmarks() -> list[str]:
    """
    Returns a list of yFinance tickers for benchmark values.

    Returns:
        list[str]: List of matching ticker symbols from yFinance.
    """
    benchmarks = (
        pl.scan_csv(paths.get_benchmark_metadata_csv_path())
        .filter((pl.col("active")=="Y")&(pl.col("source")=="yfinance"))
        .select("ticker")
        .collect()
    )
    return benchmarks["ticker"].to_list()


def get_fx_csv_sources() -> list[Path]:
    """
    Returns a list of all csv source paths for fx data

    Returns:
        list[Path]: List of all csv sources paths within the fx metadata file.
    """
    sources = (
        pl.scan_csv(paths.get_fx_metadata_csv_path())
        .filter(pl.col("source")=="local_csv")
        .select("source_file_path")
        .collect()
        .to_series()
        .to_list()
    )
    return sources


def get_active_asset_csv_sources() -> list[Path]:
    """
    Returns a list of all csv source paths for asset data

    Returns:
        list[Path]: List of all csv sources paths within the metadata file.
    """
    sources = (
        pl.scan_csv(paths.get_asset_metadata_csv_path())
        .filter((pl.col("active")=="Y") & (pl.col("source")=="local_csv"))
        .select("source_file_path")
        .collect()
        .to_series()
        .to_list()
    )
    return sources


def get_valid_benchmark_tickers(start_date: date, end_date: date) -> list[str]:
    """
    Return benchmark tickers valid within the given date range.

    A ticker is considered valid if its is active for the entire duration of the date range.

    Args:
        start_date (date): The earliest date of the desired range.
        end_date (date): The latest date of the desired range.

    Returns:
        list[str]: A list of valid benchmark ticker symbols.
    """
    metadata = (
        pl.scan_csv(paths.get_benchmark_metadata_csv_path())
        .with_columns([
            pl.col("start_date").str.strptime(pl.Date, "%Y-%m-%d"), # Convert to polars date for later comparison
            pl.col("end_date").str.strptime(pl.Date, "%Y-%m-%d")
        ])
    )
    
    valid_tickers = (
        metadata
        .filter(
            (pl.col("start_date") <= pl.lit(start_date)) & # Convert to polars date for comparison (both side must match) - pl.Lit detects it is a date
            (pl.col("end_date") >= pl.lit(end_date))
        )
        .select("ticker")
        .collect()
        .to_series()
        .to_list()
    )
    return valid_tickers



# def get_csv_ticker_source_map() -> dict[str, Path]:
#     """
#     Returns a mapping of tickers to their local CSV file paths.

#     Returns:
#         dict[str, Path]: Dictionary where keys are ticker symbols and values are local CSV file paths.
#     """
#     metadata = (
#         pl.scan_csv(paths.get_asset_metadata_csv_path())
#         .filter(pl.col("source")=="local_csv")
#         .select("ticker","source_file_path")
#         .collect()
#     )
#     return {ticker: Path(source_path) for ticker, source_path in metadata.select(["ticker","source_file_path"]).iter_rows()}


def generate_asset_metadata_json(dev_mode: bool):

    asset_csv_path = paths.get_asset_metadata_csv_path()
    asset_json_path = paths.get_asset_metadata_json_path(dev_mode)

    active_assets = []
    skipped_assets = 0

    with open(asset_csv_path, newline="", encoding="utf-8-sig") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row["active"].strip().lower() == "n":
                skipped_assets += 1
                continue
            active_assets.append({
                "ticker": row["ticker"],
                "name": row["name"],
                "asset_type": row["asset_type"],
                "currency": row["currency"],
                "start_date": row["start_date"],
                "end_date": row["end_date"],
                "dividends": row["has_dividends"]
            })

    # Create / overwrite JSON file
    with open(asset_json_path, "w", encoding="utf-8") as jsonfile:
        json.dump(active_assets, jsonfile, indent=2)

    print(f"Generated {asset_json_path}")
    print(f"{len(active_assets)} active assets written to JSON")
    print(f"{skipped_assets} inactive assets skipped")


def generate_benchmark_metadata_json(dev_mode: bool):

    benchmark_csv_path = paths.get_benchmark_metadata_csv_path()
    benchmark_json_path = paths.get_benchmark_metadata_json_path(dev_mode)

    active_benchmarks = []
    skipped_benchmarks = 0

    with open(benchmark_csv_path, newline="", encoding="utf-8-sig") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row["active"].strip().lower() == "n":
                skipped_benchmarks += 1
                continue
            active_benchmarks.append({
                "ticker": row["ticker"],
                "name": row["name"],
                "currency": row["currency"],
                "start_date": row["start_date"],
                "end_date": row["end_date"]
            })

    # Create / overwrite JSON file
    with open(benchmark_json_path, "w", encoding="utf-8") as jsonfile:
        json.dump(active_benchmarks, jsonfile, indent=2)

    print(f"Generated {benchmark_json_path}")
    print(f"{len(active_benchmarks)} active benchmarks written to JSON")
    print(f"{skipped_benchmarks} inactive benchmarks skipped")


def update_asset_metadata_csv(new_asset_data: pl.DataFrame):
    """
    Update the asset metadata CSV with new asset data.

    Computes the start and end dates for each ticker and whether it has dividends,
    then merges these updates with the existing CSV, preserving old values if new data is missing.

    Args:
        new_asset_data (pl.DataFrame): New asset data including 'ticker', 'date', and 'dividends' columns.

    Side Effects:
        Updates and overwrites the CSV file at the path returned by `paths.get_asset_metadata_csv_path()`.
    """
    # Make lazy
    new_data_lf = new_asset_data.lazy()

    asset_metadata_path = paths.get_asset_metadata_csv_path()
    asset_metadata_path.parent.mkdir(parents=True, exist_ok=True)
    
    old_csv_lf = pl.scan_csv(asset_metadata_path)

    # Compute date range and dividend presence per ticker 
    metadata_updates = (
        new_data_lf
        .group_by("ticker")
        .agg([
            pl.col("date").min().alias("start_date"),
            pl.col("date").max().alias("end_date"),
            pl.when(pl.col("dividend").is_not_null().sum() > 0)
              .then(pl.lit("Y"))
              .otherwise(pl.lit("N"))
              .alias("has_dividends")
        ])
    )

    # Join updates onto existing CSV metadata
    final_lf = old_csv_lf.join(metadata_updates, on="ticker", how="left", suffix="_new") \
        .with_columns([
            # --- Overwrites all values, even if new value is null ie. active dates will be blank if the price data is not within new compiled data
            pl.col("start_date_new").alias("start_date"),
            pl.col("end_date_new").alias("end_date"),
            pl.col("has_dividends_new").alias("has_dividends")

            # --- Coalesce preserve old value if no new value found
            # pl.coalesce(["start_date_new", "start_date"]).alias("start_date"),
            # pl.coalesce(["end_date_new", "end_date"]).alias("end_date"),
            # pl.coalesce(["has_dividends_new", "has_dividends"]).alias("has_dividends")
        ]).drop(["start_date_new", "end_date_new", "has_dividends_new"])

    # Collect and save
    final_df = final_lf.sort(["asset_type", "ticker"]).collect()
    final_df.write_csv(asset_metadata_path)
    print(f"Data updated in file at {asset_metadata_path}")


def update_benchmark_metadata_csv(new_benchmark_data: pl.DataFrame):
    """
    Update the benchmark metadata CSV with new benchmark data.

    Computes the start and end dates for each benchmark ticker and merges these updates
    with the existing CSV, preserving old values if new data is missing.

    Args:
        new_benchmark_data (pl.DataFrame): New benchmark data including 'ticker' and 'date' columns.

    Side Effects:
        Updates and overwrites the CSV file at the path returned by `paths.get_benchmark_metadata_csv_path()`.
    """
    # Make lazy
    new_data_lf = new_benchmark_data.lazy()

    benchmark_metadata_path = paths.get_benchmark_metadata_csv_path()
    benchmark_metadata_path.parent.mkdir(parents=True, exist_ok=True)
    
    old_csv_lf = pl.scan_csv(benchmark_metadata_path)

    # Compute date range based on new compiled data 
    metadata_updates = (
        new_data_lf
        .group_by("ticker")
        .agg([
            pl.col("date").min().alias("start_date"),
            pl.col("date").max().alias("end_date"),
        ])
    )

    # Join updates onto existing CSV metadata
    final_lf = old_csv_lf.join(metadata_updates, on="ticker", how="left", suffix="_new") \
        .with_columns([
            pl.col("start_date_new").alias("start_date"),
            pl.col("end_date_new").alias("end_date"),
        ]).drop(["start_date_new", "end_date_new"])

    # Collect and save
    final_df = final_lf.sort(["ticker"]).collect()
    final_df.write_csv(benchmark_metadata_path)
    print(f"Data updated in file at {benchmark_metadata_path}")
