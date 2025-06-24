from backend.utils import parse_date
from datetime import date
from dateutil.relativedelta import relativedelta
from typing import List

class Strategy:
    """
    Represents a portfolio investment strategy used during backtesting.

    Encapsulates key configuration options:
    - Whether fractional shares can be purchased.
    - How dividends are treated.
    - How often to rebalance the portfolio.
    """
    
    def __init__(
        self,
        allow_fractional_shares: bool = True,
        reinvest_dividends: bool = True,
        rebalance_frequency: str = "never"
    ):
        """
        Initialize a backtest strategy configuration.

        Args:
            allow_fractional_shares (bool): 
                Whether the portfolio allows buying fractional shares.
                
            reinvest_dividends (bool): 
                Whether dividends should be automatically reinvested into the portfolio.
                If False, dividends are added to the cash balance.
                
            rebalance_frequency (str): 
                Frequency of portfolio rebalancing. These are all relative to the start date. Options include:
                - "never": No rebalancing : assets will always be purchased based on chosen allocations
                - "daily": Rebalance each day.
                - "weekly": Rebalance each week.
                - "monthly": Rebalance each month.
                - "quarterly": Rebalance each quarter.
                - "yearly": Rebalance each year.

        """
        self.allow_fractional_shares = allow_fractional_shares
        self.reinvest_dividends = reinvest_dividends
        self.rebalance_frequency = rebalance_frequency