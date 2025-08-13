import pandas as pd
from pathlib import Path
from datetime import date
from backend.data_pipeline.ingestors import BaseIngestor
from backend.core.validators import validate_date_order
from backend.core.parsers import parse_date


class CSVIngestor(BaseIngestor):
    """
    Ingests data from a CSV file with optional date filtering.

    This class reads a CSV file located at `source_path`, expecting a 'date' column.
    It can filter the data to include only rows between `start_date` and `end_date` if provided.

    Args:
        source_path (Path): Path to the CSV file to ingest.
        start_date (date | None): Optional start date to filter data (inclusive).
        end_date (date | None): Optional end date to filter data (inclusive).

    Raises:
        ValueError: If both start_date and end_date are provided but in incorrect order.
    """


    def __init__(self,source_path: Path, start_date: date | None, end_date: date | None):
        """
        Initialize the CSVIngestor with the file path and optional date range filters.

        Args:
            source_path (Path): Path to the CSV file to be ingested.
            start_date (date | None): Optional start date to filter rows (inclusive).
            end_date (date | None): Optional end date to filter rows (inclusive).

        Raises:
            ValueError: If both `start_date` and `end_date` are provided and `start_date` > `end_date`.
        """
        if start_date and end_date:
            validate_date_order(start_date,end_date)
        self.source_path = source_path
        self.start_date = start_date
        self.end_date = end_date


    def run(self) -> pd.DataFrame:
        """
        Reads the CSV file from the specified source path, optionally filtering rows by date range.

        Parses the 'date' column as datetime (with day-first format), and filters data between
        `start_date` and `end_date` if provided.

        Raises:
            RuntimeError: If the file cannot be read, the 'date' column is missing,
                        or the resulting DataFrame is empty after filtering.

        Returns:
            pd.DataFrame: The ingested and filtered data.
        """
        print(f"Reading file : {self.source_path}...")

        try:
            data = pd.read_csv(self.source_path)
            data['date'] = data['date'].apply(parse_date)
        except Exception as e:
            raise RuntimeError(f"Error reading CSV file {self.source_path}: {e}")
        
        if 'date' not in data.columns:
            raise RuntimeError("Missing required 'date' column in CSV.")

        # Filter by start date (if present)
        if self.start_date:
            data = data[(data['date'] >= self.start_date)]

        # Filter by end date (if present)
        if self.end_date:
            data = data[(data['date'] <= self.end_date)]

        print("Read complete.")

        if data.empty:
            raise RuntimeError("Ingestion failed - no data to return")
        
        return data

