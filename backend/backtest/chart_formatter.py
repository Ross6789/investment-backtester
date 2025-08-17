import polars as pl

class ChartFormatter:

    @staticmethod
    def format_benchmark_growth(benchmark_value_df: pl.DataFrame, benchmark_metadata_df:pl.DataFrame) -> dict:
        """
        Format benchmark growth data for charts in wide format and provide a ticker-label mapping
        only for tickers present in the chart data.

        Args:
            benchmark_value_lf (DataFrame): DataFrame containing columns ["date", "ticker", "price"].
            benchmark_metadata_lf (DataFrame): DataFrame containing benchmark metadata with at least least ["ticker", "name"] columns.

        Returns:
            dict: {
                "chart_data": list[dict],  # Wide-format chart data
                "labels": dict[str, str]   # Ticker to human-readable label mapping
            }
        """
        # Convert date column to string 
        benchmark_with_string_dates_df = benchmark_value_df.with_columns(pl.col("date").dt.strftime('%Y-%m-%d').alias("date"))

        # Pivot LazyFrame into wide format
        wide_df = benchmark_with_string_dates_df.pivot(on="ticker", index="date", values="value")

        # Rename columns (remove "_price" if added by pivot)
        rename_map = {
            col: col.replace("_value", "") for col in wide_df.columns if col != "date"
        }
        wide_df = wide_df.rename(rename_map)

        # Get benchmark tickers present in chart data
        tickers_in_chart = [col for col in wide_df.columns if col != "date"]

        # Load metadata and filter for only tickers present
        metadata_df = (
            benchmark_metadata_df
            .filter(pl.col("ticker").is_in(tickers_in_chart))
            .select(["ticker", "name"])
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
    

