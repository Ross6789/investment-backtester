from datetime import date
from math import ceil
from backend.utils import validate_positive_amount
from backend.backtest.portfolios import BasePortfolio


class RealisticPortfolio(BasePortfolio):
   
    # --- Initialisation  --- 
    
    def __init__(self, backtest):
        super().__init__(backtest)
        """
        Initialize the portfolio with backtest engine.

        Args:
            backtest (Backtest): The backtest engine instance
        """
        self.dividends = []
        self.dividend_income = 0.0


    def daily_reset(self) -> None:
        """
        Reset daily-tracked portfolio attributes.
        """
        super().daily_reset()
        self.dividends = []
        self.dividend_income = 0.0

    
    # --- Trading ---

    def invest(self, ticker : str, allocated_funds : float, price : float, allow_fractional_shares: bool) -> bool:
        """
        Attempt to invest allocated funds into a specified asset.

        Args:
            ticker (str): The asset's ticker symbol.
            allocated_funds (float): Amount of cash to invest (must be > 0).
            price (float): Price per unit of the asset.
            allow_fractional_shares (bool): If True, allows partial units to be purchased.

        Returns:
            bool: True if the investment succeeds; False if insufficient funds.

        Raises:
            ValueError: If allocated_funds is not positive.
        """
        # Check funds entered is positive amount
        validate_positive_amount(allocated_funds,'allocated funds for investing')

        # Calculate amount of units which could be bought using allocated funds
        if allow_fractional_shares:
            units_bought = allocated_funds / price 
        else:
            units_bought = allocated_funds // price

        # If purchase unsuccessful ie. insufficient funds
        if units_bought <= 0:
            return False
        
        # Find total cost
        total_cost = units_bought * price
            
        # Make investment
        self.holdings[ticker] = self.holdings.get(ticker,0.0) + units_bought
        self.holdings[ticker] = self.holdings[ticker]
        self.cash_balance -= total_cost
        return True


    def sell(self,ticker : str, required_funds : float, price : float, allow_fractional_shares: bool) -> bool:
        """
        Attempt to sell holdings to raise the required amount of cash.

        Args:
            ticker (str): The asset's ticker symbol.
            required_funds (float): Amount of cash to raise (must be > 0).
            price (float): Price per unit of the asset.
            allow_fractional_shares (bool): If True, allows partial units to be sold.

        Returns:
            bool: True if the sale succeeds; False if insufficient holdings.

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
        if allow_fractional_shares:
            units_sold = min(required_funds / price, units_owned)
        else:
            units_sold = min(ceil(required_funds / price), units_owned)
        
        # If sale unsuccessful
        if units_sold <= 0:
            return False

        # Find total earnings
        total_earned = units_sold * price

        # Make sale
        self.holdings[ticker] = units_owned - units_sold
        self.holdings[ticker] = self.holdings[ticker]
        self.cash_balance += total_earned

        return True
    

    # --- Dividends ---

    def process_dividends(self, ticker_dividend_dict: dict[str, float]) -> float :
        """
        Process dividend payments for the current holdings based on provided per-ticker dividends.

        Args:
            ticker_dividend_dict (dict[str, float]): A dictionary mapping tickers to dividend amounts per unit.

        Returns:
            float: Total dividend income earned across all holdings.
                Returns 0.0 if no dividends apply to the current holdings.

        Notes:
            Updates the portfolio's dividend records with the calculated dividends for each ticker.
        """
        dividends = []
        for ticker, dividend in ticker_dividend_dict.items():
            units_held = self.holdings.get(ticker,0.0)
            if dividend is not None and units_held > 0:
                dividends.append({
                    'ticker': ticker,
                    'dividend_per_unit': dividend,
                    'total_dividend': dividend * units_held
                })
        # Defensive safety net in case method is called for date with no dividends (unlikely)
        if not dividends:
            return 0.0
        
        self.dividends = dividends
        return self._get_total_dividends()


    def _get_total_dividends(self) -> float:
        """
        Calculate the total dividend income from each of the portfolio's holdings in its current state (ie. on the current day)

        Returns:
            float: Sum of all 'total_dividend' values from the dividends list.
                Returns 0.0 if no dividends are recorded.
        """
        total = 0.0
        for div in self.dividends:
            total += div.get('total_dividend',0.0)
        return total


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
            'holdings': self._get_holdings_snapshot(date,prices),
            'dividends':self._get_dividends_snapshot(date)
        }


    def get_cash_snapshot(self, date: date) -> dict:
        """
        Create a snapshot of the portfolio's cash-related metrics for a specific date.

        Args:
            date (date): The date of the snapshot.

        Returns:
            dict: Contains date, cash balance, cash inflow, dividend income, and rebalance flag.
        """
        
        return {
            'date': date,
            'cash_balance': self.cash_balance,
            'cash_inflow': self.cash_inflow,
            'dividend_income':self.dividend_income,
            'did_rebalance':self.did_rebalance
        }
    

    def _get_dividends_snapshot(self, date: date) -> list[dict]: 
        """
        Generate a snapshot of the dividends received by the portfolio on a given date.

        Args:
            date (date): The date of the snapshot.

        Returns:
            list[dict]: A list of dividend records, each containing date, ticker,
                        dividend per unit, and total dividend.
                        Returns an empty list if no dividends were received.
        """
        # Check for divdends before returning snapshot
        if not self.dividends:
            return []
        
        return [
            {
                'date': date,
                'ticker': div['ticker'],
                'dividend_per_unit': div['dividend_per_unit'],
                'total_dividend': div['total_dividend']
            }
            for div in self.dividends
        ]


