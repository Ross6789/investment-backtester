import backend.core.paths as paths
import polars as pl

# Configuration
asset_metadata_path = paths.get_asset_metadata_path()
parquet_corporate_actions_path = paths.get_parquet_corporate_action_base_path()
csv_has_corporate_actions_path = paths.get_csv_has_corporate_action_path()

# Read parquet actions file
actions_df_eager = pl.read_parquet(parquet_corporate_actions_path)

# Read metadata file
metadata_df_eager = pl.read_csv(asset_metadata_path)

# Count actions per ticker
action_counts = (
    actions_df_eager
    .group_by('ticker')
    .agg([
        pl.col('dividends').filter(pl.col('dividends').is_not_null()).count().alias('dividend_count'),
        pl.col('stock_splits').filter(pl.col('stock_splits').is_not_null()).count().alias('stock_split_count')
    ])
    )

# Add 'Y/N' columns based on action count
action_bool = (
    action_counts.with_columns([
        pl.when(pl.col('dividend_count') > 0)
        .then(pl.lit('Y'))
        .otherwise(pl.lit('N'))
        .alias("has_dividends"),
        pl.when(pl.col('stock_split_count') > 0)
        .then(pl.lit('Y'))
        .otherwise(pl.lit('N'))
        .alias("has_stock_splits"),
    ]).sort('ticker')
)

# Merge metadata with action count
merge_df = metadata_df_eager.join(action_bool,how="left",on='ticker',suffix='_updated')
final_merged_df = merge_df.select('ticker','has_dividends_updated','has_stock_splits_updated').fill_null('N').sort('ticker')

# print dfs for visible check
print(action_bool)
print (final_merged_df)

# save to final csv to allow updating metadata
final_merged_df.write_csv(csv_has_corporate_actions_path)
print(f"Data saved to file at {csv_has_corporate_actions_path}")