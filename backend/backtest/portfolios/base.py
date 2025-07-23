from datetime import date
from backend.core.validators import validate_positive_amount
from abc import ABC, abstractmethod

class BasePortfolio(ABC):
    """
    Represents an investment portfolio with cash and holdings.

    Manages buying, selling, rebalancing assets, and tracking portfolio value.
    Supports strategies with options like fractional shares and dividend reinvestment.

    Attributes:
        cash_balance (float): The current cash available.
        holdings (dict[str, float]): Dictionary mapping ticker symbols to number of shares held.
    
    """

    # --- Initialisation and reset ---
    
    def __init__(self, backtest):
        """
        Initialize the portfolio with backtest engine.

        Args:
            backtest (Backtest): The backtest engine instance
        """
        self.backtest = backtest
        self.cash_balance = 0.0
        self.cash_inflow = 0.0
        self.did_rebalance = False
        self.holdings = {}


    def daily_reset(self) -> None:
        """
        Reset daily-tracked portfolio attributes.
        """
        self.cash_inflow = 0.0
        self.did_rebalance = False


    # --- Cash management ---

    def add_cash(self, amount: float):
        """       
        Add a cash inflow to the portfolio. Override in subclasses if cash balance should also be tracked.

        Args:
            amount (float): The amount of cash to add. Must be greater than 0.

        Raises:
            ValueError: If the amount is not positive.
        """
        validate_positive_amount(amount,'added cash')
        self.cash_balance += amount
        self.cash_inflow += amount


    def get_available_cash(self) -> float:
        """
        Return the current available cash in the portfolio.

        Returns:
            float: The cash balance.
        """
        return self.cash_balance


    # --- Trading ---

    @abstractmethod
    def invest(self, ticker : str, allocated_funds : float, price : float, allow_fractional_shares: bool) -> bool:
        pass


    @abstractmethod
    def sell(self,ticker : str, required_funds : float, price : float, allow_fractional_shares: bool) -> bool:
        pass


    # --- Valuations ---

    def get_total_value(self, prices: dict[str, float]) -> float:
        """
        Calculate the total value of current holdings.

        Args:
            prices (dict[str, float]): A dictionary mapping price column names to their values.

        Returns:
            float: The total portfolio value.
        """
        cash = self.get_available_cash()
        holding_value = 0.0
        for ticker, units in self.holdings.items():
            price = prices.get(ticker,0)
            value = units * price
            holding_value += value
        return cash + holding_value 
    

    # --- Snapshotting ---

    @abstractmethod
    def get_daily_snapshot(self, date: date, prices: dict[str, float]) -> dict:
        pass


    @abstractmethod
    def get_cash_snapshot(self, date: date) -> dict:
        pass


    def _get_holdings_snapshot(self, date: date, prices: dict[str, float]) -> list[dict]: 
        """
        Generate a snapshot of the portfolio's holdings for a specific date.

        Args:
            date (date): The date of the snapshot.
            prices (dict[str, float]): Current prices per ticker.

        Returns:
            list[dict]: A list of dictionaries, each containing date, ticker, units held, and price.
                        Returns an empty list if no holdings exist.
        """
    
        # Check for empty holdings before returning snapshot
        if not self.holdings:
            return []
        
        return [
            {
                'date': date,
                'ticker': ticker,
                'units': units,
                'base_price': prices.get(ticker)
            }
            for ticker, units in self.holdings.items()
        ]
    
