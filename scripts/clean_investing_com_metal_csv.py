import polars as pl

# Paths
read_path_gold = "/Volumes/T7/investment_backtester_data/raw/metals/investing.com/investing_com_gold.csv"
read_path_silver = "/Volumes/T7/investment_backtester_data/raw/metals/investing.com/investing_com_silver.csv"
read_path_exchange = "/Volumes/T7/investment_backtester_data/raw/metals/investing.com/investing_com_exchange.csv"
save_path_gold = "/Volumes/T7/investment_backtester_data/raw/metals/investing.com/investing_com_gold_filtered.csv"
save_path_silver = "/Volumes/T7/investment_backtester_data/raw/metals/investing.com/investing_com_silver_filtered.csv"

# Read csvs
gold_df = pl.read_csv(read_path_gold)
silver_df = pl.read_csv(read_path_silver)
exchange_df = pl.read_csv(read_path_exchange)
print("Read all csv files")

# Rename column to each dataframe ticker
gold_df = gold_df.rename({"Price":"Gold (USD)"})
silver_df = silver_df.rename({"Price":"Silver (USD)"})
exchange_df = exchange_df.rename({"Price":"GBP:USD"})

# Join exchange_rate dataframe to metal dataframes
gold_df = gold_df.join(exchange_df, on="Date",how="inner")
silver_df = silver_df.join(exchange_df,on="Date",how="inner")

# Add GBP column
gold_df = gold_df.with_columns(
    (pl.col("Gold (USD)") / pl.col("GBP:USD")).alias("Gold (GBP)")
)

silver_df= silver_df.with_columns(
    (pl.col("Silver (USD)") / pl.col("GBP:USD")).alias("Silver (GBP)")
)

# Remove obsolete columns
gold_df = gold_df.drop(["Gold (USD)","GBP:USD"])
silver_df = silver_df.drop(["Silver (USD)","GBP:USD"])

# Save csv files
gold_df.write_csv(save_path_gold)
silver_df.write_csv(save_path_silver)
print("csv files saved successfully")