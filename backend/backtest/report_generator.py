import polars as pl
from backend.models import CSVReport
from backend.utils import validate_flat_dataframe

class ReportGenerator:
    """
    Utility class for generating structured CSV reports from Polars DataFrames.

    This class transforms DataFrames into CSVReport objects, optionally including
    metadata formatted as comment lines. These reports are intended for structured
    export using the Exporter class.
    """

    @staticmethod
    def generate_csv(df: pl.DataFrame, metadata: dict[str, str] | None = None) -> CSVReport:
        """
        Generates a CSVReport with formatted metadata, headers and rows.

        Args:
            df: The Polars DataFrame to export.
            metadata: Optional dictionary to format as comments at top of file.

        Returns:
            CSVReport containing comment lines, headers, and rows.
        """
        validate_flat_dataframe(df)

        comments = ReportGenerator._format_metadata(metadata)

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
    

