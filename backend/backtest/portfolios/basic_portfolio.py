from datetime import date
from backend.core.validators import validate_positive_amount
from backend.backtest.portfolios import BasePortfolio

class BasicPortfolio(BasePortfolio):

    # --- Trading ---

    def invest(self, ticker : str, allocated_funds : float, price : float, allow_fractional_shares: bool = True) -> bool:
        """
        Invest allocated funds into a specified asset.

        Args:
            ticker (str): The asset's ticker symbol.
            allocated_funds (float): Amount of cash to invest (must be > 0).
            price (float): Price per unit of the asset.
            allow_fractional_shares (bool): If True, allows partial units to be purchased.

        Returns:
            bool: True if the investment succeeds; Should always return true for basic portfolios

        Raises:
            ValueError: If allocated_funds is not positive.
        """
        # Check funds entered is positive amount
        validate_positive_amount(allocated_funds,'allocated funds for investing')

        # Calculate amount of units which could be bought using allocated funds
        units_bought = allocated_funds / price 

        # Find total cost
        total_cost = units_bought * price
                    
        # Make investment
        self.holdings[ticker] = self.holdings.get(ticker,0.0) + units_bought
        self.cash_balance -= total_cost
        return True


    def sell(self,ticker : str, required_funds : float, price : float, allow_fractional_shares: bool = True) -> bool:
        """
        Sell holdings to raise the required amount of cash.

        Args:
            ticker (str): The asset's ticker symbol.
            required_funds (float): Amount of cash to raise (must be > 0).
            price (float): Price per unit of the asset.
            allow_fractional_shares (bool): If True, allows partial units to be sold.

        Returns:
            bool: True if the sale succeeds; Should always return true for basic portfolios

        Raises:
            ValueError: If required_funds is not positive.
        """
        # Check funds entered is positive amount
        validate_positive_amount(required_funds,'required funds for selling')

        # Find units owned and abort if none held
        units_owned = self.holdings.get(ticker,0.0)
        if units_owned <= 0:
            return False

        # Calculate how many units are to be sold
        units_sold = min(required_funds / price, units_owned)

        # Find total earning
        total_earnings = units_sold * price

        # Make sale
        self.holdings[ticker] = units_owned - units_sold
        self.cash_balance += total_earnings
        return True


    # --- Snapshotting ---

    def get_daily_snapshot(self, date: date, prices: dict[str, float]) -> dict:
        """
        Retrieve a snapshot of the portfolio's state for a given date.

        Args:
            date (date): The date for the snapshot.
            prices (dict[str, float]): Current prices per ticker.

        Returns:
            dict: Contains cash, holdings, and dividend snapshots for the date.
        """

        return {
            'cash': self.get_cash_snapshot(date),
            'holdings': self._get_holdings_snapshot(date,prices)
        }


    def get_cash_snapshot(self, date: date) -> dict:
        """
        Create a snapshot of the portfolio's cash-related metrics for a specific date.

        Args:
            date (date): The date of the snapshot.

        Returns:
            dict: Contains date, cash balance, cash inflow and rebalance flag.
        """
        
        return {
            'date': date,
            'cash_balance':self.cash_balance,
            'cash_inflow': self.cash_inflow,
            'did_rebalance':self.did_rebalance
        }
    


