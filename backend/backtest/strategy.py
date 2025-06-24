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

    def get_rebalance_dates(self, start_date: date, end_date: date, trading_dates: List[date]) -> List[date]:
        
        """
        Generate a list of rebalance dates between a start and end date based on the strategy's rebalance frequency.

        Ensures all rebalance dates fall on actual trading days by finding the first trading day 
        on or after each target rebalance date.

        Args:
            start_date (date): The start date of the backtest period.
            end_date (date): The end date of the backtest period.
            trading_dates (List[date]): A list of all valid trading dates.

        Returns:
            List[date]: A list of dates on which rebalancing should occur, aligned to trading days.

        Raises:
            ValueError: If an invalid rebalance frequency is provided.
        """
        
        rebalance_dates = []
        target_date = start_date
        
        while target_date <= end_date:
            rebalance_date = next((td for td in trading_dates if td >= target_date),None)
            if rebalance_date == None:
                break
            rebalance_dates.append(rebalance_date)
            
            # Determine next target rebalance date based on strategy
            match self.rebalance_frequency.lower():
                case "never":
                    break
                case "daily":
                    target_date += relativedelta(days=1)
                case "weekly":
                    target_date += relativedelta(days=7)
                case "monthly":
                    target_date += relativedelta(months=1)
                case "quarterly":
                    target_date += relativedelta(months=3)
                case "yearly":
                    target_date += relativedelta(years=1)
                case _: 
                    raise ValueError(f"Invalid rebalance frequency : {self.rebalance_frequency}")

        return rebalance_dates
    