import backend.config as config
import polars as pl

# Configuration
actions_path = config.get_corporate_action_data_path()
metadata_path = config.get_asset_metadata_path()

# Read parquet actions file
actions_df_eager = pl.read_parquet(actions_path)

# Read csv metadata file
metadata_df_eager = pl.read_csv(metadata_path)

action_counts = (
    actions_df_eager
    .group_by('ticker')
    .agg([
        pl.col('dividends').filter(pl.col('dividends').is_not_null()).count().alias('dividend_count'),
        pl.col('stock_splits').filter(pl.col('stock_splits').is_not_null()).count().alias('stock_split_count')
    ])
    )

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
    ])
)

# print(action_counts)
print(action_bool)
print(metadata_df_eager)