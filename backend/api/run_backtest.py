import backend.core.paths as paths
import polars as pl
from pathlib import Path
from datetime import date
from backend.backtest.data_loader import get_backtest_data, get_benchmark_data
from backend.core.models import TargetPortfolio, RecurringInvestment, BacktestConfig, Strategy
from backend.core.enums import BacktestMode, BaseCurrency, RebalanceFrequency, ReinvestmentFrequency
from backend.core.parsers import parse_enum, parse_date
from backend.backtest.runner import BacktestRunner
from backend.core.paths import get_benchmark_metadata_csv_path


def run_backtest(input_data: dict) -> dict:
   
    # Parse input
    mode = parse_enum(BacktestMode, input_data["mode"])
    base_currency = parse_enum(BaseCurrency, input_data["base_currency"])
    start_date = parse_date(input_data["start_date"])
    end_date = parse_date(input_data["end_date"])
    target_weights = input_data["target_weights"]
    initial_investment = input_data["initial_investment"]

    strategy_data = input_data["strategy"]
    fractional_shares=strategy_data["fractional_shares"]
    reinvest_dividends=strategy_data["reinvest_dividends"]
    rebalance_frequency=parse_enum(RebalanceFrequency, strategy_data["rebalance_frequency"])

    recurring_data = input_data["recurring_investment"]
    if recurring_data is not None:
        recurring_investment_amount=recurring_data["amount"]
        recurring_investment_frequency = parse_enum(ReinvestmentFrequency, recurring_data["frequency"])
        recurring_investment = RecurringInvestment(recurring_investment_amount,recurring_investment_frequency)
    else:
        recurring_investment = None
        
    # Create config objects
    target_portfolio = TargetPortfolio(target_weights)
    strategy = Strategy(fractional_shares,reinvest_dividends,rebalance_frequency)
    backtest_config = BacktestConfig(start_date,end_date,target_portfolio,mode,base_currency,strategy,initial_investment,recurring_investment)

    # Fetch data for backtest
    backtest_data = get_backtest_data(mode,base_currency,target_portfolio.get_tickers(),start_date,end_date)

    # Fetch data for benchmark - only fetching data for benchmarks active across full period
    benchmark_metadata_path = get_benchmark_metadata_csv_path()
    benchmark_tickers = _get_valid_benchmark_tickers(start_date,end_date, benchmark_metadata_path)
    benchmark_data = get_benchmark_data(base_currency,benchmark_tickers,start_date,end_date)

    # Create and run backtest
    backtest = BacktestRunner(backtest_config, backtest_data, benchmark_data, benchmark_metadata_path, paths.get_backtest_run_base_path())
    results = backtest.run()

    return {
        "settings":input_data,
        "results":results
    }

# Determine which benchmarks are active for the full backtest period
def _get_valid_benchmark_tickers(start_date: date, end_date: date, benchmark_metadata_path: Path) -> list[str]:
    
    metadata = (
        pl.scan_csv(benchmark_metadata_path)
        .with_columns([
            pl.col("start_date").str.strptime(pl.Date, "%d/%m/%Y"), # Convert to polars date for later comparison
            pl.col("end_date").str.strptime(pl.Date, "%d/%m/%Y")
        ])
    )

    
    valid_tickers = (
        metadata
        .filter(
            (pl.col("start_date") <= pl.lit(start_date)) & # Convert to polars date for comparison (both side must match) - pl.Lit detects it is a date
            (pl.col("end_date") >= pl.lit(end_date))
        )
        .select("ticker")
        .collect()
        .to_series()
        .to_list()
    )
    
    return valid_tickers