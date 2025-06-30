from backend.pipelines.data_processor import process_price_data,process_corporate_action_data
import backend.config as config
import polars as pl

# Process price data - ie. forward fill prices
processed_prices = process_price_data(config.get_parquet_price_base_path())

# Process corporate action data
corporate_actions = process_corporate_action_data(config.get_parquet_corporate_action_base_path()) 

# Merge
compiled = processed_prices.join(corporate_actions, on=['date','ticker'],how='left')

print(processed_prices)
print(corporate_actions)
print(compiled)

