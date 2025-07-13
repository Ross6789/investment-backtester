# import backend.config as config
# from datetime import date
# from backend.backtest.data_loader import get_backtest_data
# from backend.models import TargetPortfolio, RecurringInvestment, BacktestConfig, Strategy
# from backend.enums import BacktestMode, BaseCurrency, RebalanceFrequency, ReinvestmentFrequency
# from backend.backtest.runner import BacktestRunner
# from backend.backtest.exporter import BacktestExporter

# # config
# csv_save_path = config.get_csv_backtest_result_path()

# # User choices
# mode = BacktestMode.REALISTIC
# base_currency = BaseCurrency.GBP
# start_date = date.fromisoformat("2020-01-01")
# end_date = date.fromisoformat("2025-01-01")
# target_weights = {'AAPL':0.5,'GOOG':0.5}
# initial_investment = 10000
# fractional_shares = False
# reinvest_dividends = False
# rebalance_frequency = RebalanceFrequency.MONTHLY
# recurring_investment_amount = 2500
# recurring_investment_frequency = ReinvestmentFrequency.MONTHLY

# # Create objects
# target_portfolio = TargetPortfolio(target_weights)
# strategy = Strategy(fractional_shares,reinvest_dividends,rebalance_frequency)
# recurring_investment = RecurringInvestment(recurring_investment_amount,recurring_investment_frequency)
# backtest_config = BacktestConfig(start_date,end_date,target_portfolio,mode,base_currency,strategy,initial_investment,recurring_investment)

# configuration_dict = {
#     "Mode" : mode,
#     "Base Currency" : base_currency.value,
#     "Start date": start_date.isoformat(),
#     "End date": end_date.isoformat(),
#     "Target weights": target_weights,
#     "Initial amount": initial_investment,
#     "Recurring investment amount": None if recurring_investment == None else recurring_investment.amount,
#     "Recurring investment frequency": None if recurring_investment == None else recurring_investment_frequency,
#     "Fractional shares allowed": fractional_shares,
#     "Reinvest dividends": reinvest_dividends,
#     "Rebalance frequency": rebalance_frequency
# }

# # Fetch data for backtest
# backtest_data = get_backtest_data(mode,base_currency,target_portfolio.get_tickers(),start_date,end_date)

# # Create and run backtest
# backtest = BacktestRunner(backtest_config, backtest_data)
# print('starting backtest...')
# result = backtest.run()

# # export results
# result = BacktestExporter(result)
# print('Exporting results to csv...')
# result.to_csv(csv_save_path,configuration_dict)



import backend.config as config
from datetime import date
from backend.backtest.data_loader import get_backtest_data
from backend.models import TargetPortfolio, RecurringInvestment, BacktestConfig, Strategy
from backend.enums import BacktestMode, BaseCurrency, RebalanceFrequency, ReinvestmentFrequency
from backend.backtest.runner import BacktestRunner
from backend.backtest.exporter import BacktestExporter

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
backtest = BacktestRunner(backtest_config, backtest_data)
print('starting backtest...')
result = backtest.run()

save_path = config.get_parquet_backtest_result_path()

print(result.data.schema)
print(result.calendar.schema)
print(result.cash.schema)
print(result.holdings.schema)
print(result.dividends.schema)
print(result.orders.schema)


result.data.write_parquet(save_path / 'backtest_data.parquet')
result.calendar.write_parquet(save_path / 'backtest_calendar.parquet')
result.cash.write_parquet(save_path / 'backtest_cash.parquet')
result.holdings.write_parquet(save_path / 'backtest_holdings.parquet')
result.dividends.write_parquet(save_path / 'backtest_dividends.parquet')
result.orders.write_parquet(save_path / 'backtest_orders.parquet')
