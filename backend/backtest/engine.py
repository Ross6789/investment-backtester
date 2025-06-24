import backend.config as config
import polars as pl
from datetime import date
from typing import Dict, Tuple, List
from backend.pipelines.data_loader import get_price_data
from backend.backtest.portfolio import Portfolio
from backend.backtest.strategy import Strategy


# config
parquet_price_path = config.get_parquet_price_base_path()
start_date = date.fromisoformat("2024-01-01")
end_date = date.fromisoformat("2025-01-01")
tickers = ['AAPL','GOOG']
initial_balance = 10000
strategyA = Strategy(allow_fractional_shares=True, reinvest_dividends=True,rebalance_frequency='monthly')
strategyB = Strategy(allow_fractional_shares=False, reinvest_dividends=True,rebalance_frequency='never')

price_data= get_price_data(parquet_price_path,tickers,start_date,end_date)

portfolioA = Portfolio(initial_balance, strategyA)
PortfolioB = Portfolio(initial_balance, strategyB)

PortfolioTargets = {
    'AAPL':0.5,
    'GOOG':0.5
}

def backtest(portfolio: Portfolio, price_data : Tuple[pl.DataFrame, List[date]], target_weights: Dict[str, float]):
    
    # unpack price data tuple
    all_prices, trading_dates = price_data
    
    # Create empty list for portfolio snapshots
    snapshots = []

    # Find rebalance dates
    rebalance_dates = portfolio.strategy.get_rebalance_dates(start_date,end_date,trading_dates)

    # Iterate through date range and rebalance where necessary
    for row in all_prices.iter_rows(named=True):
        date = row['date']
        date_prices = {k: v for k, v in row.items() if k != 'date'}
    
        if date in rebalance_dates:
            portfolio.rebalance(target_weights,date_prices)
            print(portfolio.snapshot(date,date_prices))


backtest(portfolioA,price_data,PortfolioTargets)