import backend.config as config
import polars as pl
from typing import Dict
from backend.pipelines.data_loader import get_price_data
from backend.backtest.portfolio import Portfolio
from backend.backtest.strategy import Strategy


# config
parquet_price_path = config.get_parquet_price_base_path()
start_date = "2024-01-01"
end_date = "2025-01-01"
tickers = ['AAPL','GOOG']
initial_balance = 10000
strategyA = Strategy(allow_fractional_shares=True, reinvest_dividends=True,rebalance_frequency='yearly')
strategyB = Strategy(allow_fractional_shares=False, reinvest_dividends=True,rebalance_frequency='never')

prices = get_price_data(parquet_price_path,tickers,start_date,end_date)
print(prices)

portfolioA = Portfolio(initial_balance, strategyA)
PortfolioB = Portfolio(initial_balance, strategyB)

PortfolioTargets = {
    'AAPL':0.5,
    'GOOG':0.5
}

def backtest(portfolio: Portfolio, prices : pl.DataFrame, target_weights: Dict[str, float]):
    
    snapshots = []
    previous_rebalance_date = None

    for row in prices.iter_rows(named=True):
        date = row['date']
        prices = {k: v for k, v in row.items() if k != 'date'}
    
        if portfolio.strategy.should_rebalance(previous_rebalance_date,date):
            print(f"{date} : rebalance")

backtest(portfolioA,prices,PortfolioTargets)