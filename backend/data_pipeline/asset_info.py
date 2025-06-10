import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..")))

import polars as pl
import config.config as config

# Retrieve specific column from metadata csv
def get_metadata(column: str) -> list[str]:
    # Read the CSV eagerly to inspect and clean column names
    df = pl.read_csv(config.get_asset_metadata_path())

    print(config.get_asset_metadata_path())

    # Clean column names
    cleaned_columns = {col: col.strip().capitalize() for col in df.columns}
    df = df.rename(cleaned_columns)

    # Clean requested column
    cleaned_column = column.strip().capitalize()

    if cleaned_column not in df.columns:
        raise ValueError(f"Column '{column}' not found in metadata.")

    return df[cleaned_column].to_list()

