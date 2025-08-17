import polars as pl
from pathlib import Path
from backend.core.models import BacktestConfig
from backend.utils.scheduling import generate_recurring_dates
from backend.backtest.chart_formatter import ChartFormatter
import backend.backtest.data_cache as cache

class BenchmarkSimulator:

    @staticmethod
    def run(config: BacktestConfig, benchmark_data: pl.DataFrame) -> dict:
        """
        Run a benchmark simulation and return formatted chart data.

        Args:
            config (BacktestConfig): Simulation configuration including start/end dates and cashflows.
            benchmark_data (pl.DataFrame): Benchmark price data with columns ['date', 'ticker', 'price'].

        Returns:
            dict: Formatted benchmark chart data including wide format and label mapping.
        """
        benchmark_results_df = BenchmarkSimulator._simulate_backtest_for_benchmarks(config, benchmark_data)
        benchmark_chart_data = ChartFormatter.format_benchmark_growth(benchmark_results_df,cache.get_cached_benchmarks_metadata())
        return benchmark_chart_data


    @staticmethod
    def _simulate_backtest_for_benchmarks(config: BacktestConfig, benchmark_data: pl.DataFrame) -> pl.DataFrame:
        """
        Simulate benchmark portfolio growth over time.

        Args:
            config (BacktestConfig): Configuration for the backtest including cashflows and date range.
            benchmark_data (pl.DataFrame): Benchmark price data with columns ['date', 'ticker', 'price'].

        Returns:
            pl.DataFrame: DataFrame with columns ['date', 'ticker', 'value'] representing simulated benchmark value.
        """

        # --- Generate LazyFrame of all cashflows

        # Initial investment
        cashflow_dates_df = pl.DataFrame({
            "date": [config.start_date],
            "cashflow": [config.initial_investment]
        })

        # Recurring investment if applicable
        if config.recurring_investment:
            dates = sorted(generate_recurring_dates(config.start_date,config.end_date, config.recurring_investment.frequency.value))
            recurring_lf = pl.DataFrame({
                "date": dates,
                "cashflow": [config.recurring_investment.amount] * len(dates)
            })
            cashflow_dates_df = pl.concat([cashflow_dates_df, recurring_lf]).sort("date")

        # Find units purchased on every date
        cashflow_with_prices_df = cashflow_dates_df.join(benchmark_data,on="date",how="left")
        units_df = cashflow_with_prices_df.with_columns((pl.col("cashflow")/pl.col("price")).alias("units"))
        
        # Find cumulative units on every cashflow date - group by ticker to ensure counts are restricted to each benchmark ie. benchmarks are only cum_sum their own units and not other benchmarks
        cumulative_units_df = units_df.with_columns(pl.col('units').cum_sum().over('ticker').alias('cumulative_units'))

        # join benchmark data (already filtered for date range and forward filled previously) to unit data
        full_dates_units_df = benchmark_data.join(cumulative_units_df, on=["date","ticker","price"],how="left")

        # Forward fill units
        filled_df = full_dates_units_df.fill_null(strategy="forward")

        # Find total value using price x units
        benchmark_values_df = filled_df.with_columns((pl.col("cumulative_units")*pl.col("price")).alias("value"))
        final_benchmark_df = benchmark_values_df.select(["date","ticker","value"])

        # Print funtions used to debug error : * Error found : Not grouping ticker when accumulating units: therefore S&P benchmark was counting units from FTSE *
        # print(cashflow_dates_lf.collect())
        # print(cashflow_with_prices_lf.collect())
        # print(cumulative_units_lf.collect())
        # print(full_dates_units_lf.collect())
        # print(filled_lf.collect())
        # print(benchmark_values_lf.collect())
        # print(final_benchmark_lf.collect())

        return final_benchmark_df.sort(['ticker','date'])

