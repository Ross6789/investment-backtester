from typing import Dict
from datetime import date
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
            price = prices.get(self.get_price_col_name(ticker))
            value = units * price
            total_value += value
        return round(total_value,2)

    def rebalance(self, target_weights: Dict[str, float], prices: Dict[str, float]):
        # Get starting balance
        cash_balance = self.cash_balance

        # Sell all assets to get total balance
        for ticker, units in self.holdings.items():
            price = prices.get(self.get_price_col_name(ticker))
            value = units * price
            cash_balance += value
        
        # Reset holdings
        self.holdings = {}

        # Initialise remaining balance
        remaining_balance = cash_balance

        # Buy assets using defined target weights
        for ticker, weight in target_weights.items():
            balance_available = cash_balance * weight
            price = prices.get(self.get_price_col_name(ticker))
            if self.strategy.allow_fractional_shares:
                units_bought = floor((balance_available / price)*10000)/10000 # use a factor and floor to round down to 4 decimal places
            else:
                units_bought = balance_available // price
            self.holdings[ticker] = units_bought
            remaining_balance -= units_bought * price

        # Update cash balance
        self.cash_balance = round(remaining_balance,2) 
        
    def snapshot(self, date: date, prices: Dict[str, float]):
        snapshot = {
            'date': date.isoformat(),
            'holdings': self.holdings.copy(),
            'cash balance': self.cash_balance,
            'total_value': self.get_value(prices)
        }
        return snapshot

    def get_price_col_name(self, ticker: str) -> str:
        if self.strategy.reinvest_dividends:
            price_type = 'close'
        else:
            price_type = 'adj_close'
        return f'{price_type}_{ticker}'