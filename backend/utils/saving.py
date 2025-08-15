import polars as pl
from datetime import datetime
from pathlib import Path


def generate_timestamp() -> str:
    """
    Generate a timestamp string representing the current date and time.

    Returns:
        str: The current timestamp in the format 'YYYYMMDD_HHMMSS'.
    """
    return datetime.now().strftime('%Y%m%d_%H%M%S')


def save_partitioned_parquet(data : pl.DataFrame, directory_save_path: Path) -> None:
    """
    Save a Polars DataFrame as a partitioned Parquet dataset, grouped by ticker.

    Each group (by 'ticker') is saved in its own subdirectory, suitable for efficient querying.

    Args:
        data (pl.DataFrame): The DataFrame to save. Must contain a 'ticker' column.
        directory_save_path (Path): Root directory to save the partitioned dataset.

    Raises:
        RuntimeError: If any error occurs while writing the Parquet files.
    """
    try:
        for (ticker,) , ticker_df in data.group_by("ticker"):
            folder = directory_save_path / f"ticker={ticker}"
            folder.mkdir(parents=True, exist_ok=True)
            ticker_df.write_parquet(folder / "data.parquet")
        print(f"Data saved to {directory_save_path}.") 
    except Exception as e:
        raise RuntimeError(f"Failed to save partitioned parquet to {directory_save_path}: {e}") from e


def save_regular_parquet(data : pl.DataFrame, save_path: Path) -> None:
    """
    Save a Polars DataFrame as a single flat Parquet file.

    Args:
        data (pl.DataFrame): The DataFrame to save.
        save_path (Path): The full file path where the Parquet file should be saved.

    Raises:
        RuntimeError: If saving fails for any reason.
    """
    try:
        data.write_parquet(save_path)
        print(f"Data saved to {save_path}.") 
    except Exception as e:
        raise RuntimeError(f"Failed to save regular parquet to {save_path}: {e}") from e


def save_csv(data : pl.DataFrame, save_path: Path) -> None: 
    """
    Save a Polars DataFrame as a CSV file.

    Args:
        data (pl.DataFrame): The DataFrame to save.
        save_path (Path): The full file path where the CSV should be saved.

    Raises:
        RuntimeError: If writing the CSV file fails.
    """
    try:
        data.write_csv(save_path)
        print(f"Data saved to {save_path}.") 
    except Exception as e:
        raise RuntimeError(f"Failed to save CSV to {save_path}: {e}") from e
  

import tempfile
import threading
import os
import pandas as pd

def save_report_temporarily(report_sheets : dict[str, pl.DataFrame], prefix="backtest_report_"):
    print("running temp export code")
    # Create a temporary file
    temp_file = tempfile.NamedTemporaryFile(prefix=prefix, suffix=".xlsx", delete=False)
    temp_file_path = temp_file.name
    temp_file.close()  # close so pandas can write

    # Open an Excel writer context - needed to write multi page workbook (to_csv will write all on one page)
    with pd.ExcelWriter(temp_file_path, engine='xlsxwriter', datetime_format="dd/mm/yyyy") as writer:

        for name, report in report_sheets.items():
            report.to_pandas().to_excel(writer,sheet_name=name, index=False)

    print(f"saving temp_report a : {temp_file_path}")

    # Schedule deletion in 10 minutes
    def delete_temp_file(path):
        try:
            os.remove(path)
            print(f"Deleted temporary file: {path}")
        except FileNotFoundError:
            pass

    timer = threading.Timer(interval=600, function=delete_temp_file, args=[temp_file_path])
    timer.start()

    return temp_file_path



