import csv
import polars as pl
from pathlib import Path
from datetime import datetime
from backend.models import CSVReport



class Exporter:

    def __init__(self, base_path : Path, timestamp : str):
        self.timestamped_folder = self._create_timestamped_folder(base_path,timestamp)
    
    @staticmethod
    def _create_timestamped_folder(base_path: Path, timestamp: str) -> Path:
        new_folder_path = base_path / timestamp
        new_folder_path.mkdir(parents = True, exist_ok=False)
        return new_folder_path
    

    def save_report_to_csv(self, csv_report: CSVReport, file_name: str) -> None:
        
        # Generate full save path
        save_path = self.timestamped_folder / 'csv' / f'{file_name}.csv'

        # Create the directory if it doesn't exist
        save_path.parent.mkdir(parents=True, exist_ok=True)
    
        with open(save_path, mode='w') as f:
                writer = csv.writer(f)

                # Write backtest_configuration as comments at top of csv file
                for line in csv_report.comments:
                    writer.writerow([line])
                writer.writerow([])  # Empty line between configuration and notes

                # Add notes discussing accuracy of round
                writer.writerow(["# IMPORTANT NOTE: Values in this report are rounded for display. Minor discrepancies may occur when performing manual calculations due to the backtest engine using higher floating-point precision."])  # Empty line between configuration and results
                writer.writerow([])  # Empty line between notes and results

                # Write main body of report
                writer.writerow(csv_report.headers)
                writer.writerows(csv_report.rows)

        print(f'Exported {file_name} to : {save_path}')

    def save_dataframe_to_csv(self, dataframe: pl.DataFrame, file_name: str) -> None:
        
        # Generate full save path
        save_path = self.timestamped_folder / 'csv' / f'{file_name}.csv'

        # Create the directory if it doesn't exist
        save_path.parent.mkdir(parents=True, exist_ok=True)
    
        # Write to csv
        dataframe.write_csv(save_path)
        print(f'Exported {file_name} to : {save_path}')

