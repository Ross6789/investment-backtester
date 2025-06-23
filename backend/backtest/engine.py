import backend.config as config
from backend.pipelines.data_loader import get_price_data
from backend.backtest.portfolio import Portfolio
from backend.backtest.strategy import Strategy


# config
parquet_price_path = config.get_parquet_price_base_path()
start_date = "2024-01-01"
end_date = "2025-01-01"
tickers = ['AAPL','GOOG']
initial_balance = 10000
strategyA = Strategy(allow_fractional_shares=True, reinvest_dividends=True,rebalance_frequency='never')
strategyB = Strategy(allow_fractional_shares=False, reinvest_dividends=True,rebalance_frequency='never')

prices = get_price_data(parquet_price_path,tickers,start_date,end_date)
print(prices)

portfolioA = Portfolio(initial_balance, strategyA)
PortfolioB = Portfolio(initial_balance, strategyB)

PortfolioTargets = {
    'AAPL':0.5,
    'GOOG':0.5
}

Prices = {
    'AAPL':100,
    'GOOG':60
}

Date = '2025-01-01'

portfolioA.rebalance(PortfolioTargets,Prices)
print(f"Portfolio A : {portfolioA.snapshot(Date, Prices)}")

PortfolioB.rebalance(PortfolioTargets,Prices)
print(f"Portfolio B : {PortfolioB.snapshot(Date, Prices)}")
