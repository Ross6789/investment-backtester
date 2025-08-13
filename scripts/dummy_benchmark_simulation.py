import backend.core.paths as paths
from datetime import date
from backend.backtest.data_loader import get_benchmark_data, get_benchmark_metadata_csv_path
from backend.core.models import TargetPortfolio, RecurringInvestment, BacktestConfig, Strategy
from backend.core.enums import BacktestMode, BaseCurrency, RebalanceFrequency, ReinvestmentFrequency
from backend.backtest.benchmark_simulator import BenchmarkSimulator
from backend.core.paths import EXTERNAL_DATA_BASE_PATH

# User choices
mode = BacktestMode.BASIC
base_currency = BaseCurrency.GBP
start_date = date.fromisoformat("2025-06-01")
end_date = date.fromisoformat("2025-06-30")
target_weights = {'VUSA.L':1}
initial_investment = 10000
fractional_shares = True
reinvest_dividends = True
rebalance_frequency = RebalanceFrequency.MONTHLY
recurring_investment_amount = 2500
recurring_investment_frequency = ReinvestmentFrequency.MONTHLY

# Create objects
target_portfolio = TargetPortfolio(target_weights)
strategy = Strategy(fractional_shares,reinvest_dividends,rebalance_frequency)
recurring_investment = RecurringInvestment(recurring_investment_amount,recurring_investment_frequency)
backtest_config = BacktestConfig(start_date,end_date,target_portfolio,mode,base_currency,strategy,initial_investment,recurring_investment)

# Fetch data for benchmarking
benchmark_tickers = ["^FTSE","^GSPC"]
benchmark_data = get_benchmark_data(base_currency,benchmark_tickers,start_date,end_date)


# Create and run backtest
results = BenchmarkSimulator.run(backtest_config, benchmark_data, get_benchmark_metadata_csv_path())
print(results)

