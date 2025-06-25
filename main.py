import backend.config as config
import polars as pl
from datetime import date
from backend.pipelines.data_loader import get_price_data
from backend.backtest.portfolio import Portfolio
from backend.backtest.strategy import Strategy
from backend.models import RecurringInvestment
from backend.backtest.engine import BacktestEngine


# config
parquet_price_path = config.get_parquet_price_base_path()

# User choices
start_date = date.fromisoformat("2024-01-01")
end_date = date.fromisoformat("2025-01-01")
target_weights = {'AAPL':0.5,'GOOG':0.5}
initial_balance = 10000
recurring_cashflow = {'amount':100, 'frequency':'monthly'}
fractional_shares = True
reinvest_dividends = True
rebalance_frequency = 'monthly'
recurring_investment = RecurringInvestment(1000,'quarterly')

# Instantiate engine objects
strategy = Strategy(fractional_shares, reinvest_dividends, rebalance_frequency)
portfolio = Portfolio(initial_balance, strategy)

# Retreive price data
tickers = list(target_weights.keys())
price_data= get_price_data(parquet_price_path,tickers,start_date,end_date)

# run backtest
backtest = BacktestEngine(portfolio,start_date,end_date,target_weights,price_data,recurring_investment)
history = backtest.run()

# print history
print(history)

