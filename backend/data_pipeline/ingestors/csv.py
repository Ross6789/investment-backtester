import pandas as pd
from pathlib import Path
from datetime import date


class CSVIngestor:
    def __init__(self,source_path: Path, start_date: date, end_date: date):
        if start_date and end_date:
            if start_date > end_date:
                raise ValueError("Start date must be after the end date")
        self.source_path = source_path
        self.start_date = start_date
        self.end_date = end_date

    # Method to download data from csv
    def run(self) -> pd.DataFrame:
        print(f"Reading file : {self.source_path}...")
        all_data = pd.read_csv(self.source_path, parse_dates=['date'], dayfirst=True)
        # Convert start and end date to pandas for comparison
        start_date_pd = pd.Timestamp(self.start_date)
        end_date_pd = pd.Timestamp(self.end_date)
        # Ingest only rows within the date range
        raw_data = all_data[(all_data['date'] >= start_date_pd) & (all_data['date'] <= end_date_pd)]
        print("Read complete.")
        if raw_data.empty:
            raise ValueError(f"No data in {self.source_path} within the date range.")
        return raw_data

