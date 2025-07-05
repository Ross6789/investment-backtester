from typing import Dict,Tuple
from datetime import date
from math import ceil

from backend.utils import round_down, validate_positive_amount


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
        self.dividends = None
        self.dividend_income = 0.0
        self.did_receive_dividends = False
        self.did_rebalance = False
        self.did_buy = False
        self.did_sell = False
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
            value = round_down(units * price,2)
            total_value += value
        return total_value
    
    def get_available_cash(self) -> float:
        return self.cash
        
    def snapshot(self, date: date, prices: Dict[str, float]):

        # Calculate the value of each different holding
        holding_values = {
            ticker : round_down(self.holdings.get(ticker, 0) * prices.get(ticker,0),2)
            for ticker in self.holdings
        }

        snapshot = {
            'date': date.isoformat(),
            'cash_balance': self.cash,
            'cash_inflow': self.cash_inflow,
            'dividend_income':self.dividend_income,
            'total_value': self.get_value(prices),
            'holdings': self.holdings.copy(),
            'prices': prices.copy(),
            'holding_values':holding_values.copy(),
            'dividends': None if self.dividends is None else self.dividends.copy(),
            'did_receive_dividends': self.did_receive_dividends,
            'did_rebalance': self.did_rebalance,
            'did_buy': self.did_buy,
            'did_sell': self.did_sell
        }
        return snapshot
    
    def invest(self,ticker : str, allocated_funds : float, price : float, allow_fractional_shares: bool):

        # Check funds entered is positive amount
        # validate_positive_amount(allocated_funds,'allocated funds for investing')

        # Buy assets using allocated funds
        if allow_fractional_shares:
            units_bought = allocated_funds / price
        else:
            units_bought = allocated_funds // price
        self.holdings[ticker] = self.holdings.get(ticker,0.0) + units_bought
        total_cost = round(units_bought * price,2)
            
        # Round units to 4 DP 
        self.holdings[ticker] = round_down(self.holdings[ticker],4)

        # Update cash balance
        self.cash -= total_cost

        # Update buy flag
        self.did_buy = True

    def sell(self,ticker : str, required_funds : float, price : float, allow_fractional_shares: bool):

        # Check funds entered is positive amount
        # validate_positive_amount(required_funds,'required funds for selling')

        # Find units owned to ensure that units sold does not exceed units owned
        units_owned = self.holdings.get(ticker,0.0)

        # Sell assets to gain allocated funds
        if allow_fractional_shares:
            units_sold = min(required_funds / price, units_owned)
        else:
            units_sold = min(ceil(required_funds / price), units_owned)
        self.holdings[ticker] = units_owned - units_sold
        total_earned = round(units_sold * price,2)
            
        # Round units to 4 DP 
        self.holdings[ticker] = round_down(self.holdings[ticker],4)

        # Update cash balance
        self.cash += total_earned

        # Update sold flag
        self.did_sell = True

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
    
    def process_dividends(self, dividend_ticker_dict: Dict[str, float]) -> float :
        dividends = []
        tickers = dividend_ticker_dict.keys()
        for ticker in tickers:
            div_per_unit = dividend_ticker_dict.get(ticker)
            units_held = self.holdings.get(ticker,0.0)
            if div_per_unit is not None and units_held > 0:
                dividends.append({
                    'ticker': ticker,
                    'dividend_per_unit': div_per_unit,
                    'total_dividend': round_down(div_per_unit * units_held,2)
                })
        if dividends:
            self.dividends = dividends
            self.did_receive_dividends = True
        
        return self.get_total_dividends()

    def get_total_dividends(self) -> float:
        """
        Calculate the total dividend income from each of the portfolio's holdings in its current state (ie. on the current day)

        Returns:
            float: Sum of all 'total_div_income' values from the dividends list.
                Returns 0.0 if no dividends are recorded.
        """
        total = 0.0
        if self.dividends:
            for div in self.dividends:
                total += div.get('total_dividend',0.0)
        return total
    
    def daily_reset(self) -> None:
        self.cash_inflow = 0.0
        self.dividends = None
        self.did_receive_dividends = False
        self.dividend_income = 0.0
        self.did_buy = False
        self.did_sell = False
        self.did_rebalance = False


