from backend.utils.metadata import update_asset_metadata_csv
import polars as pl

compiled_data = pl.read_parquet("/Volumes/T7/investment_backtester_data/compiled/parquet/historical-prices")
update_asset_metadata_csv(compiled_data)