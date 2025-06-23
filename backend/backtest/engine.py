import backend.config as config
from backend.pipelines.data_loader import get_price_data
from backend.backtest.portfolio import Portfolio

# config
parquet_price_path = config.get_parquet_price_base_path()
start_date = "2024-01-01"
end_date = "2025-01-01"
tickers = ['AAPL','GOOG']

prices = get_price_data(parquet_price_path,tickers,start_date,end_date)
print(prices)

portfolioA = Portfolio(10000)
PortfolioB = Portfolio(50000)

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
