import backend.core.paths as paths
import polars as pl

# Configuration
asset_metadata_path = paths.get_asset_metadata_path()
parquet_price_path = paths.get_backtest_data_path()
csv_ticker_date_range_path = '/Volumes/T7/investment_backtester_data/processed/csv/find_ticker_date_range.csv'

# Read parquet actions file
prices_df_eager = pl.read_parquet(parquet_price_path)

# Read metadata file
metadata_df_eager = pl.read_csv(asset_metadata_path)

# Find date range for each ticker
ticker_date_range = (
    prices_df_eager
    .group_by('ticker')
    .agg([
        pl.col('date').min().alias('start_date'),
        pl.col('date').max().alias('end_date')
    ])
    )

# Merge metadata with price date range
merge_df = metadata_df_eager.join(ticker_date_range, how="left",on='ticker',suffix='_updated')
final_merged_df = merge_df.select('ticker','start_date_updated','end_date_updated').sort('ticker')

# print dfs for visible check
print(final_merged_df)

# save to final csv to allow updating metadata
final_merged_df.write_csv(csv_ticker_date_range_path)
print(f"Data saved to file at {csv_ticker_date_range_path}")