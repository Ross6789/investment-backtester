import polars as pl
from backend.core.models import CSVReport, RoundingConfig
from backend.core.validators import validate_flat_dataframe
from backend.utils.dataframes import convert_columns_to_percentage, round_dataframe_columns

class ReportGenerator:
    """
    Utility class for generating structured CSV reports from Polars DataFrames.

    This class transforms DataFrames into CSVReport objects, optionally including
    metadata formatted as comment lines. These reports are intended for structured
    export using the Exporter class.
    """

    @staticmethod
    def _format_for_readability(df: pl.DataFrame, percentify_cols: list[str] | None = None, rounding_config: RoundingConfig | None = None) -> pl.DataFrame:
        """
        Improves readability of a DataFrame for reporting by optionally converting selected columns to percentages and rounding float values based on semantic roles.

        Args:
            df (pl.DataFrame): The input DataFrame to format.
            percentify_cols (list[str] | None): List of column names to convert to percentages.
            rounding_config (RoundingConfig | None): Optional configuration for rounding precision. If not provided, default rounding settings will be used.

        Returns:
            pl.DataFrame: A formatted DataFrame with improved readability.
        """
        # Convert to percentages (replace original columns)
        if percentify_cols:
            df = convert_columns_to_percentage(df,percentify_cols)

        # Apply rounding
        df = round_dataframe_columns(df, rounding_config)

        return df


    @staticmethod
    def generate_csv(df: pl.DataFrame, metadata: dict[str, str] | None = None, percentify_cols: list[str] | None = None, rounding_config : RoundingConfig | None = None) -> CSVReport:
        """
        Generates a structured CSVReport from a Polars DataFrame, optionally
        including metadata and applying formatting for readability.

        Args:
            df (pl.DataFrame): The DataFrame to export.
            metadata (dict[str, str] | None): Optional dictionary to include as comments at the top of the CSV file.
            percentify_cols (list[str] | None): Optional list of columns to convert to percentages for the report.
            rounding_config (RoundingConfig | None): Optional rounding precision configuration. If not provided, defaults will be applied.

        Returns:
            CSVReport: An object containing comment lines, headers, and rows
            suitable for structured CSV export.
        """
        validate_flat_dataframe(df)

        comments = ReportGenerator._format_metadata(metadata)
        df = ReportGenerator._format_for_readability(df,percentify_cols, rounding_config)

        return CSVReport(
            comments=comments,
            headers=df.columns,
            rows=df.rows()
        )
    

    @staticmethod
    def _format_metadata(metadata: dict[str, str] | None) -> list[str]:
        """
        Generates a CSVReport object from a Polars DataFrame, with optional metadata.

        Args:
            df (pl.DataFrame): The Polars DataFrame to be exported.
            metadata (dict[str, str] | None): Optional metadata to be included as comments at the top of the report.

        Returns:
            CSVReport: An object containing comment lines, headers, and rows for CSV export.

        Raises:
            AssertionError: If the DataFrame does not have a flat (non-nested) structure.
        """
        if not metadata:
            return []
        return [f"# {key}: {value} \n" for key, value in metadata.items()]
    

