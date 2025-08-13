import polars as pl
from pathlib import Path

class ChartFormatter:

    @staticmethod
    def format_benchmark_growth(benchmark_value_lf: pl.LazyFrame, benchmark_metadata_path: Path) -> dict:
        """
        Format benchmark growth data for charts in wide format and provide a ticker-label mapping
        only for tickers present in the chart data.

        Args:
            benchmark_value_lf (pl.LazyFrame): LazyFrame containing columns ["date", "ticker", "price"].
            benchmark_metadata_path (Path): Path to the benchmark metadata CSV file containing at least ["ticker", "name"] columns.

        Returns:
            dict: {
                "chart_data": list[dict],  # Wide-format chart data
                "labels": dict[str, str]   # Ticker to human-readable label mapping
            }
        """
        # Pivot LazyFrame into wide format
        print(benchmark_value_lf.collect())
        wide_df = benchmark_value_lf.collect().pivot(on="ticker", index="date", values="value")

        # Rename columns (remove "_price" if added by pivot)
        rename_map = {
            col: col.replace("_value", "") for col in wide_df.columns if col != "date"
        }
        wide_df = wide_df.rename(rename_map)

        # Get benchmark tickers present in chart data
        tickers_in_chart = [col for col in wide_df.columns if col != "date"]

        # Load metadata and filter for only tickers present
        metadata_df = (
            pl.scan_csv(benchmark_metadata_path)
            .filter(pl.col("ticker").is_in(tickers_in_chart))
            .select(["ticker", "name"])
            .collect()
        )

        # Create ticker -> "ticker - name" mapping
        labels = {
            row["ticker"]: f'{row["ticker"]} - {row["name"]}'
            for row in metadata_df.iter_rows(named=True)
        }

        return {
            "data": wide_df.to_dicts(),
            "labels": labels
        }
    

