from backend.utils.metadata import update_asset_metadata_csv
from backend.core.paths import BACKEND_DATA_ROOT_PATH
import polars as pl

compiled_data = pl.scan_parquet("/Volumes/T7/investment_backtester_data/inputs/dev/historical-prices.parquet")
# compiled_data = pl.scan_parquet(BACKEND_DATA_ROOT_PATH / "backtests" / "inputs" / "prod" / "historical-prices.parquet")
crypto = compiled_data.filter(pl.col('ticker')=="AAPL").collect()
print(crypto)

# update_asset_metadata_csv(compiled_data) 
fx = pl.scan_parquet("/Volumes/T7/investment_backtester_data/inputs/dev/benchmarks.parquet").filter((pl.col('ticker')=="^FTSE")&(pl.col('currency')=="USD")).collect()
print(fx)

# update_asset_metadata_csv()