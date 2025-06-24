from backend.utils import parse_date
from datetime import date
from dateutil.relativedelta import relativedelta

class Strategy:
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

    def should_rebalance(self, previous_rebalance_date: str | None, current_date: str) -> bool:
        
        """
        Decide if we should rebalance on current_date, given the last rebalance date.
        Args:
            previous_rebalance_date: YYYY-MM-DD string or None if never rebalance yet.
            current_date: YYYY-MM-DD string or date object for today.
        Returns:
            bool: True if rebalance should occur today.
        """
                
        # Rebalance if missing last rebalance date ie. always rebalance first day
        if previous_rebalance_date is None:
            return True
        
        # Parse into date objects
        previous_rebalance_date = parse_date(previous_rebalance_date)
        current_date = parse_date(current_date)

        # Determine rebalance status based on strategy
        match self.rebalance_frequency.lower():
            case "never":
                return False
            case "daily":
                return True
            case "weekly":
                if current_date == previous_rebalance_date + relativedelta(days=7):
                    return True
                else: 
                    return False
            case "monthly":
                if current_date == previous_rebalance_date + relativedelta(months=1):
                    return True
                else:
                    return False
            case "quarterly":
                if current_date == previous_rebalance_date + relativedelta(months=3):
                    return True
                else: 
                    False
            case "yearly":
                if current_date == previous_rebalance_date + relativedelta(years=1):
                    return True
                else:
                    return False
            case _: 
                raise ValueError(f"Invalid rebalance frequency : {self.rebalance_frequency}")