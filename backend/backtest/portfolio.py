from typing import Dict, List
from math import floor
from backend.backtest.strategy import Strategy

class Portfolio:
    def __init__(self, initial_balance : float, strategy : Strategy):
        self.cash_balance = initial_balance
        self.strategy = strategy
        self.holdings = {}

    def get_value(self, prices: Dict[str, float]):
        total_value = self.cash_balance
        for ticker, units in self.holdings.items():
            price = prices.get(ticker)
            value = units * price
            total_value += value
        return total_value

    def rebalance(self, target_weights: Dict[str, float], prices: Dict[str, float]):
        # Get starting balance
        cash_balance = self.cash_balance

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
            if self.strategy.allow_fractional_shares:
                units_bought = balance_available / price
            else:
                units_bought = floor(balance_available / price)
            self.holdings[ticker] = units_bought
            remaining_balance -= units_bought * price

        # Update remaining balance
        self.cash_balance = remaining_balance
        
    def snapshot(self, date: str, prices: Dict[str, float]):
        snapshot = {
            'date': date,
            'holdings': self.holdings.copy(),
            'cash balance': self.cash_balance,
            'total_value': self.get_value(prices)
        }
        return snapshot

