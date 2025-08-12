import polars as pl
from backend.core.models import BacktestConfig
from backend.utils.scheduling import generate_recurring_dates

class BenchmarkSimulator:

    @staticmethod
    def simulate_benchmarks(config: BacktestConfig, benchmark_data: pl.LazyFrame):
        
        # Generate LazyFrame of all cashflows
        
        # Initial investment
        cashflow_dates_lf = pl.LazyFrame({
            "date": [config.start_date],
            "cashflow": [config.initial_investment]
        })

        # Recurring investment if applicable
        if config.recurring_investment:
            dates = sorted(generate_recurring_dates(config.start_date,config.end_date, config.recurring_investment.frequency.value))
            recurring_lf = pl.LazyFrame({
                "date": dates,
                "cashflow": [config.recurring_investment.amount] * len(dates)
            })
            cashflow_dates_lf = pl.concat([cashflow_dates_lf, recurring_lf])

        # Find units purchased on every date
        cashflow_with_prices_lf = cashflow_dates_lf.join(benchmark_data,on="date",how="left")
        units_lf = cashflow_with_prices_lf.with_columns((pl.col("cashflow")/pl.col("price")).alias("units"))
        
        # Find cumulative units on every cashflow date
        cumulative_units_lf = units_lf.with_columns(pl.col("units").cum_sum().alias("cumulative_units"))

        # join benchmark data (already filtered for date range and forward filled previously) to unit data
        full_dates_units_lf = benchmark_data.join(cumulative_units_lf, on="date",how="left")

        # Forward fill units
        filled_lf = full_dates_units_lf.fill_null(strategy="forward")

        # Find total value using price x units
        benchmark_values_lf = filled_lf.with_columns((pl.col("cumulative_units")*pl.col("price")).alias("value"))
        final_benchmark_lf = benchmark_values_lf.select(["date","ticker","value"])

        print(cashflow_dates_lf.collect())
        print(cumulative_units_lf.collect())
        print(benchmark_values_lf.collect())

        return final_benchmark_lf.sort(['ticker','date']).collect()
