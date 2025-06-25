from typing import Dict
from datetime import date
from math import floor
from backend.backtest.strategy import Strategy

class Portfolio:
    """
    Represents an investment portfolio with cash and holdings.

    Manages buying, selling, rebalancing assets, and tracking portfolio value.
    Supports strategies with options like fractional shares and dividend reinvestment.

    Attributes:
        cash_balance (float): The current cash available.
        holdings (Dict[str, float]): Dictionary mapping ticker symbols to number of shares held.
        strategy (Strategy): The investment strategy being followed.
    
    """
        
    def __init__(self, initial_balance : float, strategy : Strategy):
        """
        Initialize the portfolio with starting cash and strategy.

        Args:
            initial_cash (float): Initial amount of cash in the portfolio.
            strategy (Strategy): The investment strategy instance.
        """
        self.cash_balance = initial_balance
        self.strategy = strategy
        self.holdings = {}
        

    def get_value(self, prices: Dict[str, float]) -> float:
        """
        Calculate the total portfolio value based on current holdings and cash balance.

        Args:
            prices (Dict[str, float]): A dictionary mapping price column names to their values.

        Returns:
            float: The total portfolio value rounded to 2 decimal places.
        """
        total_value = self.cash_balance
        for ticker, units in self.holdings.items():
            price = prices.get(self.get_price_col_name(ticker))
            value = units * price
            total_value += value
        return round(total_value,2)
    

    def rebalance(self, target_weights: Dict[str, float], prices: Dict[str, float]):
        """
        Rebalance the portfolio holdings according to target weights and current prices.

        Sells all existing holdings to cash, then buys assets based on target weights.
        Supports fractional shares rounding down to 4 decimal places if allowed.

        Args:
            target_weights (Dict[str, float]): Target portfolio weights by ticker (e.g. {'AAPL': 0.5}).
            prices (Dict[str, float]): Current prices keyed by price column name (e.g. 'close_AAPL').
        """
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
        """
        Create a snapshot of the portfolio state at a given date.

        Args:
            date (date): The date of the snapshot.
            prices (Dict[str, float]): Current prices keyed by price column name.

        Returns:
            Dict[str, object]: A dictionary containing date (ISO format), holdings, cash balance, and total portfolio value.
        """
        snapshot = {
            'date': date.isoformat(),
            'holdings': self.holdings.copy(),
            'cash balance': self.cash_balance,
            'total_value': self.get_value(prices)
        }
        return snapshot

    def get_price_col_name(self, ticker: str) -> str:
        """
        Determine the price column name to use based on the strategy's dividend reinvestment setting.

        Args:
            ticker (str): The ticker symbol of the asset.

        Returns:
            str: The price column name in the format '{price_type}_{ticker}'.
        """
        if self.strategy.reinvest_dividends:
            price_type = 'close'
        else:
            price_type = 'adj_close'
        return f'{price_type}_{ticker}'

    def add_cash(self, amount: float):
        """
        Add a specified amount to the holdings cash balance.

        Args:
            amount (float): The amount of cash to add. Must be greater than 0.

        Raises:
            ValueError: If the amount is not positive.
        """
        if amount <= 0:
            raise ValueError("Invalid amount : must be greater than zero")
        self.cash_balance += amount
    
    