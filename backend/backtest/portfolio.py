from typing import Dict,List
from datetime import date
from math import ceil

from backend.utils import validate_positive_amount


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
        
    def __init__(self, backtest_engine):
        """
        Initialize the portfolio with backtest engine.

        Args:
            backtest_engine (BacktestEngine): The backtest engine instance
        """
        self.backtest_engine = backtest_engine
        self.cash = 0.0
        self.cash_inflow = 0.0
        self.dividends = []
        self.dividend_income = 0.0
        self.did_rebalance = False
        self.holdings = {}

    def get_value(self, prices: Dict[str, float]) -> float:
        """
        Calculate the total portfolio value based on current holdings and cash balance.

        Args:
            prices (Dict[str, float]): A dictionary mapping price column names to their values.

        Returns:
            float: The total portfolio value rounded to 2 decimal places.
        """
        total_value = self.cash
        for ticker, units in self.holdings.items():
            price = prices.get(ticker,0)
            value = units * price
            total_value += value
        return total_value
    
    def get_available_cash(self) -> float:
        return self.cash
        
    def _get_cash_snapshot(self, date: date) -> Dict:
        
        return {
            'date': date,
            'cash_balance': self.cash,
            'cash_inflow': self.cash_inflow,
            'dividend_income':self.dividend_income 
        }
    
    def _get_holdings_snapshot(self, date: date, prices: dict[str, float]) -> List[Dict]: 
    
        # Check for empty holdings before returning snapshot
        if not self.holdings:
            return []
        
        return [
            {
                'date': date,
                'ticker': ticker,
                'units': units,
                'price': prices.get(ticker)
            }
            for ticker, units in self.holdings.items()
        ]

    def _get_dividends_snapshot(self, date: date) -> List[Dict]: 
        
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

    def get_daily_snapshot(self, date: date, prices: Dict[str, float]) -> Dict:

        return {
            'cash': self._get_cash_snapshot(date),
            'holdings': self._get_holdings_snapshot(date,prices),
            'dividends':self._get_dividends_snapshot(date)
        }
    
    def invest(self,ticker : str, allocated_funds : float, price : float, allow_fractional_shares: bool) -> bool:

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
        self.cash -= total_cost
        return True


    def sell(self,ticker : str, required_funds : float, price : float, allow_fractional_shares: bool) -> bool:

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
        self.cash += total_earned

        return True

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
    
    def process_dividends(self, ticker_dividend_dict: Dict[str, float]) -> float :
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
    
    def daily_reset(self) -> None:
        self.cash_inflow = 0.0
        self.dividends = []
        self.dividend_income = 0.0
        self.did_rebalance = False


