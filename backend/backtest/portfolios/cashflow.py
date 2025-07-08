from datetime import date
from math import ceil
from backend.utils import validate_positive_amount
from abc import ABC, abstractmethod
from backend.backtest.portfolios.base import BasePortfolio


class CashflowPortfolio(BasePortfolio):
   
    # --- Initialisation and reset --- 
    
    def __init__(self, backtest):
        super().__init__(backtest)
        """
        Initialize the portfolio with backtest engine.

        Args:
            backtest (Backtest): The backtest engine instance
        """
        self.cash = 0.0
        self.cash_inflow = 0.0
        self.dividends = []
        self.dividend_income = 0.0


    def daily_reset(self) -> None:
        """
        Reset daily-tracked portfolio attributes.
        """
        super().daily_reset()
        self.cash_inflow = 0.0
        self.dividends = []
        self.dividend_income = 0.0


    # --- Cash management ---

    def add_cash(self, amount: float):
        """
        Add a specified amount to the holdings cash balance.

        Args:
            amount (float): The amount of cash to add. Must be greater than 0.

        Raises:
            ValueError: If the amount is not positive.
        """
        validate_positive_amount(amount,'added cash')
        self.cash += amount
        self.cash_inflow += amount


    def get_available_cash(self) -> float:
        """
        Return the current available cash in the portfolio.

        Returns:
            float: The cash balance.
        """
        return self.cash