import csv
import os 
import polars as pl
import pandas as pd
from pathlib import Path
from backend.core.models import CSVReport
from backend.utils.dataframes import round_dataframe_columns, flatten_dataframe_columns

class Exporter:
    """
    Handles exporting of backtest results and reports to a timestamped directory structure.

    The Exporter creates a unique timestamped folder within the given base path and
    provides methods to export both structured CSV reports and raw Polars DataFrames.

    Folder structure:
    - base_path / <timestamp> / reports / csv / ...
    - base_path / <timestamp> / results / csv / ...
    """

    def __init__(self, base_path : Path, timestamp : str):
        """
        Initializes the exporter with a timestamped folder.

        Args:
            base_path (Path): Root output directory.
            timestamp (str): Timestamp string used to uniquely identify this run.
        """
        self.timestamped_folder = self._create_timestamped_folder(base_path,timestamp)
    

    @staticmethod
    def _create_timestamped_folder(base_path: Path, timestamp: str) -> Path:
        """
        Creates a new subfolder for the current backtest run using the timestamp.

        Args:
            base_path (Path): Root output directory.
            timestamp (str): Timestamp used for folder naming.

        Returns:
            Path: The path to the newly created timestamped folder.

        Raises:
            FileExistsError: If a folder with the same timestamp already exists.
        """
        new_folder_path = base_path / timestamp
        new_folder_path.mkdir(parents = True, exist_ok=False)
        return new_folder_path
    

    def save_report_to_csv(self, csv_report: CSVReport, file_name: str) -> None:
        """
        Saves a structured report (with comments and metadata) to a CSV file.

        Args:
            csv_report (CSVReport): A report object containing headers, rows, and comments.
            file_name (str): Name of the file (without extension).

        Notes:
            - Writes configuration and notes as comments at the top of the CSV file.
            - Adds a disclaimer about rounding precision.
        """
        # Generate full save path
        save_path = self.timestamped_folder / 'reports' / 'csv' / f'{file_name}.csv'

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
        """
        Saves a raw Polars DataFrame to CSV, with rounding applied for display.

        Args:
            dataframe (pl.DataFrame): The DataFrame to be saved.
            file_name (str): Name of the file (without extension).

        Notes:
            - Values are rounded for improved readability in the exported CSV.
        """
        # Generate full save path
        save_path = self.timestamped_folder / 'results' / 'csv' / f'{file_name}.csv'

        # Create the directory if it doesn't exist
        save_path.parent.mkdir(parents=True, exist_ok=True)

        # Flatten nested lists in dataframe (convert to str)
        flatted_df = flatten_dataframe_columns(dataframe)

        # Round dataframe
        rounded_df = round_dataframe_columns(flatted_df)
    
        # Write to csv
        rounded_df.write_csv(save_path)
        print(f'Exported {file_name} to : {save_path}')


    def save_dataframes_to_excel_workbook(self, name_dataframe_mappings : dict[str,pl.DataFrame], file_name: str) -> None:

        # Create save folder
        output_dir = self.timestamped_folder / 'results' / 'excel'
        os.makedirs(output_dir, exist_ok=True)  # Creates the directory if it doesn't exist

        # Generate full save path
        save_path = output_dir / f'{file_name}.xlsx'

        # Open an Excel writer context
        with pd.ExcelWriter(save_path, engine='xlsxwriter', datetime_format="dd/mm/yyyy") as writer:

            for name, report in name_dataframe_mappings.items():
                report.to_pandas().to_excel(writer,sheet_name=name, index=False)