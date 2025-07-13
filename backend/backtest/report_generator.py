import polars as pl
from backend.models import CSVReport
from backend.utils import validate_flat_dataframe


class ReportGenerator:

    ROUNDING_MAP = {
        "units": 0,
        "value": 2,
        "total_dividend": 4,
        "base_price": 4,
        "cash_balance": 2,
        "total_portfolio_value": 2,
        # Add all potentially used columns here


    }

    @staticmethod
    def generate_csv(df: pl.DataFrame, metadata: dict[str, str] = None) -> CSVReport:
        """
        Generates a CSVReport with formatted metadata and rounded numeric values.

        Args:
            df: The Polars DataFrame to export.
            metadata: Optional dictionary to format as comments at top of file.

        Returns:
            CSVReport containing comment lines, headers, and rows.
        """
        validate_flat_dataframe(df)

        df = ReportGenerator._round_columns(df)

        comments = ReportGenerator._format_metadata(metadata)

        return CSVReport(
            comments=comments,
            headers=df.columns,
            rows=df.rows()
        )


    @staticmethod
    def _round_columns(df: pl.DataFrame) -> pl.DataFrame:
        exprs = []
        for col, decimals in ReportGenerator.ROUNDING_MAP.items():
            if col in df.columns:
                exprs.append(pl.col(col).round(decimals).alias(col))
        return df.with_columns(exprs) if exprs else df


    @staticmethod
    def _format_metadata(metadata: dict[str, str] | None) -> list[str]:
        if not metadata:
            return []
        return [f"# {key}: {value}" for key, value in metadata.items()]
    

test_df = pl.DataFrame({
    "id": [1, 2],
    "person": ['Alice','Bob'],
    "scores": [30,40],
    "hair": ['blond','brown']
})

print(test_df)

print(ReportGenerator.generate_csv(test_df))

