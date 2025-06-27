import backend.config as config
import polars as pl
from datetime import date
from backend.pipelines.data_loader import get_price_data,get_corporate_action_data
from backend.models import PortfolioWeights,RecurringInvestment
from backend.backtest.engine import BacktestEngine
from backend.backtest.result import BacktestResult


# config
parquet_price_path = config.get_parquet_price_base_path()
parquet_corporate_action_path = config.get_parquet_corporate_action_base_path()
csv_save_path = config.get_csv_backtest_result_path()

# User choices
mode = 'manual'
start_date = date.fromisoformat("2020-01-01")
end_date = date.fromisoformat("2023-01-01")
target_weights = PortfolioWeights({'AAPL':0.5,'GOOG':0.5}).weights
initial_balance = 10000
fractional_shares = False
reinvest_dividends = True
rebalance_frequency = 'monthly'
recurring_investment = RecurringInvestment(2500,'monthly')

configuration_dict = {
    "Mode" : mode,
    "Start date": start_date.isoformat(),
    "End date": end_date.isoformat(),
    "Target weights": target_weights,
    "Initial amount": initial_balance,
    "Recurring investment amount": None if recurring_investment == None else recurring_investment.amount,
    "Recurring investment frequency": None if recurring_investment == None else recurring_investment.frequency,
    "Fractional shares allowed": fractional_shares,
    "Reinvest dividends": reinvest_dividends,
    "Rebalance frequency": rebalance_frequency
}

# Retreive price data
tickers = list(target_weights.keys())
price_data= get_price_data(parquet_price_path,tickers,start_date,end_date)

# Retrieve corporate action data
corporate_action_data = get_corporate_action_data(parquet_corporate_action_path,tickers,start_date,end_date)

# Create and run backtest
backtest = BacktestEngine(start_date,end_date,target_weights,price_data,corporate_action_data,mode,initial_balance,recurring_investment,fractional_shares,reinvest_dividends,rebalance_frequency)
result = BacktestResult(backtest.run())

# export results
result.to_csv(csv_save_path,configuration_dict)
