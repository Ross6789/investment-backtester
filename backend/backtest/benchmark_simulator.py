import polars as pl
from backend.core.models import BacktestConfig

class BenchmarkSimulator:

    @staticmethod
    def simulate_benchmarks(config: BacktestConfig, benchmark_data: pl.LazyFrame) -> dict:
        # Generate date lf
        all_dates_lf = pl.LazyFrame(pl.date_range(self.config.start_date, self.config.end_date))

        # Generate cashflow lf
        cashflow_dates_lf = pl.LazyFrame({
            "date": [self.config.start_date],
            "cashflow": [self.config.initial_investment]
        })

        if self.cashflow_dates:
            dates = sorted(self.cashflow_dates)
            recurring_lf = pl.LazyFrame({
                "date": dates,
                "cashflow": [self.config.recurring_investment.amount] * len(self.cashflow_dates)
            })

            cashflow_dates_lf = pl.concat([cashflow_dates_lf, recurring_lf])

        # Find units purchased on every date
        joined_lf = cashflow_dates_lf.join(benchmark_data,on="date",how="left")
        units_lf = joined_lf.with_columns(pl.col("cashflow")/pl.col("price").alias("units"))

        # join all dates lf with units lf
        dates_units_lf = all_dates_lf.join(units_lf, on="date",how="left")

        # Forward fill units
        filled_lf = dates_units_lf.fill_null(strategy="forward")

        # Find total value using price x units
        benchmark_values_lf = filled_lf.with_columns(pl.col("units")*pl.col("price").alias("value")).select(["date","value"])

        print(benchmark_values_lf.collect())
