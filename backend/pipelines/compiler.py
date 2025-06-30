from backend.pipelines.data_processor import process_price_data
import backend.config as config

processed_df = process_price_data(config.get_parquet_price_base_path())
print(processed_df)