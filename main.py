import backend.core.paths as paths
from datetime import date
from backend.backtest.data_loader import get_backtest_data
from backend.core.models import TargetPortfolio, RecurringInvestment, BacktestConfig, Strategy
from backend.core.enums import BacktestMode, BaseCurrency, RebalanceFrequency, ReinvestmentFrequency
from backend.backtest.runner import BacktestRunner

# User choices
mode = BacktestMode.REALISTIC
base_currency = BaseCurrency.GBP
start_date = date.fromisoformat("2020-01-01")
end_date = date.fromisoformat("2025-01-01")
target_weights = {'AAPL':0.5,'GOOG':0.5}
initial_investment = 10000
fractional_shares = False
reinvest_dividends = False
rebalance_frequency = RebalanceFrequency.MONTHLY
recurring_investment_amount = 2500
recurring_investment_frequency = ReinvestmentFrequency.MONTHLY

# Create objects
target_portfolio = TargetPortfolio(target_weights)
strategy = Strategy(fractional_shares,reinvest_dividends,rebalance_frequency)
recurring_investment = RecurringInvestment(recurring_investment_amount,recurring_investment_frequency)
backtest_config = BacktestConfig(start_date,end_date,target_portfolio,mode,base_currency,strategy,initial_investment,recurring_investment)

# Fetch data for backtest
backtest_data = get_backtest_data(mode,base_currency,target_portfolio.get_tickers(),start_date,end_date)

# Create and run backtest
backtest = BacktestRunner(backtest_config, backtest_data,paths.get_backtest_run_base_path())
backtest.run()

