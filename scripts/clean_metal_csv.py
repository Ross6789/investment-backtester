import polars as pl

# Paths
read_path = "/Volumes/T7/investment_backtester_data/raw/yahoo_metals.csv"
save_path = "/Volumes/T7/investment_backtester_data/raw/yahoo_metals_filtered.csv"

# Read csv
df = pl.read_csv(read_path)

 # Drop rows with missing price data
df_filtered = df.filter(pl.col('Gold (GBP)') != 0)

# Save csv
df_filtered.write_csv(save_path)