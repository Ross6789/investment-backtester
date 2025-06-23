from typing import Dict, List
import math

# Config
fractional_shares = False

class Portfolio:
    def __init__(self, cash_gbp : float):
        self.cash_gbp = cash_gbp
        self.holdings = {}

    def get_value(self, prices: Dict[str, float]):
        total_value = self.cash_gbp
        for ticker, units in self.holdings.items():
            price = prices.get(ticker)
            value = units * price
            total_value += value
        return total_value

    def rebalance(self, target_weights: Dict[str, float], prices: Dict[str, float]):
        # Get starting balance
        cash_balance = self.cash_gbp

        # Sell all assets to get total balance
        for ticker, units in self.holdings.items():
            price = prices.get(ticker)
            value = units * price
            cash_balance += value

        # Record remaining balance
        remaining_balance = cash_balance

        # Buy assets using defined target weights
        for ticker, weight in target_weights.items():
            balance_available = cash_balance * weight
            price = prices.get(ticker)
            if fractional_shares:
                units_bought = balance_available / price
            else:
                units_bought = math.floor(balance_available / price)
            self.holdings[ticker] = units_bought
            remaining_balance -= units_bought * price

        # Update remaining balance
        self.cash_gbp = remaining_balance
        
    def snapshot(self, date: str, prices: Dict[str, float]):
        snapshot = {
            'date': date,
            'holdings': self.holdings.copy(),
            'cash balance': self.cash_gbp,
            'total_value': self.get_value(prices)
        }
        return snapshot

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
