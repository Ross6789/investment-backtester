import backend.config as config
import polars as pl
from datetime import date
from backend.pipelines.loader import get_backtest_data
from backend.models import TargetPortfolio, RecurringInvestment, BacktestConfig, Strategy
from backend.backtest.engine import BacktestEngine
from backend.backtest.result import BacktestResult

# config
csv_save_path = config.get_csv_backtest_result_path()

# User choices
mode = 'manual'
start_date = date.fromisoformat("1950-01-01")
end_date = date.fromisoformat("2025-01-01")
target_weights = {'AAPL':0.5,'GOOG':0.5}
initial_investment = 10000
fractional_shares = False
reinvest_dividends = True
rebalance_frequency = 'monthly'
recurring_investment_amount = 2500
recurring_investment_frequency = 'monthly'

# Create objects
target_portfolio = TargetPortfolio(target_weights)
strategy = Strategy(fractional_shares,reinvest_dividends,rebalance_frequency)
recurring_investment = RecurringInvestment(recurring_investment_amount,recurring_investment_frequency)
backtest_config = BacktestConfig(mode,strategy,initial_investment,recurring_investment)

configuration_dict = {
    "Mode" : mode,
    "Start date": start_date.isoformat(),
    "End date": end_date.isoformat(),
    "Target weights": target_weights,
    "Initial amount": initial_investment,
    "Recurring investment amount": None if recurring_investment == None else recurring_investment.amount,
    "Recurring investment frequency": None if recurring_investment == None else recurring_investment.frequency,
    "Fractional shares allowed": fractional_shares,
    "Reinvest dividends": reinvest_dividends,
    "Rebalance frequency": rebalance_frequency
}

# Fetch data for backtest
backtest_data = get_backtest_data(mode,target_portfolio.get_tickers(),start_date,end_date)

# Create and run backtest
backtest = BacktestEngine(start_date,end_date,backtest_data,target_portfolio,backtest_config)
print('starting backtest...')
history = backtest.run()
print('finished backtest. Analysing results...')
result = BacktestResult(history)

# export results
print('finished analysing. Exporting results to csv...')
result.to_csv(csv_save_path,configuration_dict)

