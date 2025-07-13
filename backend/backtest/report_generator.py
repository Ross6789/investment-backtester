import polars as pl
from backend.models import CSVReport
from backend.utils import validate_flat_dataframe


class ReportGenerator:

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
        if not metadata:
            return []
        return [f"# {key}: {value}" for key, value in metadata.items()]
    

# test_df = pl.DataFrame({
#     "id": [1, 2],
#     "person": ['Alice','Bob'],
#     "scores": [30,40],
#     "hair": ['blond','brown']
# })

# print(test_df)

# print(ReportGenerator.generate_csv(test_df))

