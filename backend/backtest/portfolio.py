from typing import Dict,Tuple
from datetime import date
from backend.models import Strategy
from backend.utils import round_down


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
        
    def __init__(self, strategy : Strategy, backtest_engine):
        """
        Initialize the portfolio with starting cash and strategy.

        Args:
            strategy (Strategy): The investment strategy instance.
            backtest_engine (BacktestEngine): The backtest engine instance
        """
        self.strategy = strategy
        self.backtest_engine = backtest_engine
        self.cash = 0
        self.holdings = {}
        self.dividends = []
        

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
            price = prices.get(self.get_price_col_name(ticker))
            value = units * price
            total_value += value
        return round(total_value,2)
    
    def get_available_cash(self) -> float:
        return self.cash

    def invest_by_target(self, target_weights: Dict[str, float], prices: Dict[str, float]):
        """
        Invests the portfolio's available cash according to target weights and current prices.

        This method does not rebalance existing holdings, but uses only the available cash balance
        to buy assets according to the given target weights. Supports fractional shares (rounded 
        down to 4 decimal places) if allowed by the strategy.

        Args:
            target_weights (Dict[str, float]): Target portfolio weights by ticker (e.g. {'AAPL': 0.5}).
            prices (Dict[str, float]): Current prices keyed by price column name (e.g. {'close_AAPL': 187.3}).
        """
        # Get starting balance and initialise remaining balance
        starting_balance = self.cash
        remaining_balance = starting_balance

        # Buy assets using defined target weights
        for ticker, weight in target_weights.items():
            balance_available = starting_balance * weight
            price = prices.get(self.get_price_col_name(ticker))
            if self.strategy.allow_fractional_shares:
                units_bought = floor((balance_available / price)*10000)/10000 # use a factor and floor to round down to 4 decimal places
            else:
                units_bought = balance_available // price
            self.holdings[ticker] = self.holdings.get(ticker,0.0) + units_bought
            remaining_balance -= units_bought * price
            
            # Round units to 4 DP - might have accrued more digits in addition
            self.holdings[ticker] = floor(self.holdings[ticker]*10000)/10000

        # Update cash balance
        self.cash = round(remaining_balance,2) 

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
        cash_balance = self.cash

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
            if self.backtest_engine.strategy.allow_fractional_shares:
                units_bought = floor((balance_available / price)*10000)/10000 # use a factor and floor to round down to 4 decimal places
            else:
                units_bought = balance_available // price
            self.holdings[ticker] = units_bought
            remaining_balance -= units_bought * price

        # Update cash balance
        self.cash = round(remaining_balance,2) 
        
    def snapshot(self, date: date, prices: Dict[str, float], cash_inflow: float, dividend_income: float, rebalanced: bool, invested: bool, dividends_received: bool):
        """
        Create a snapshot of the portfolio state at a given date.

        Args:
            date (date): The date of the snapshot.
            prices (Dict[str, float]): Current prices keyed by ticker.
            cash_inflow (float): New cash added on this date.
            dividend_income (float): Dividend income received and not reinvested
            rebalanced (bool): Whether a rebalance occurred on this date.
            invested (bool): Whether a recurring investment was made.
            div

        Returns:
            Dict[str, object]: Snapshot of the portfolio state.
        """
        # Condense the price dicts to contain only the applicable price (ie. adj close or close) keyed to each ticker
        ticker_prices = {
            ticker : round(prices.get(self.get_price_col_name(ticker),0),4)
            for ticker in self.holdings
        }

        # Calculate the value of each different holding
        holding_values = {
            ticker : round(self.holdings.get(ticker, 0) * ticker_prices.get((ticker), 0),2)
            for ticker in self.holdings
        }

        snapshot = {
            'date': date.isoformat(),
            'cash_balance': self.cash,
            'cash_inflow': cash_inflow,
            'dividend_income':dividend_income,
            'total_value': self.get_value(prices),
            'holdings': self.holdings.copy(),
            'prices': ticker_prices.copy(),
            'holding_values':holding_values.copy(),
            'dividends':self.dividends.copy(),
            'rebalanced': rebalanced,
            'invested': invested,
            'dividends_received': dividends_received
            # 'stock_splits': 

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
        if self.backtest_engine.mode == "manual":
            price_type = 'close'
        else:
            price_type = 'adj_close'
        return f'{price_type}_{ticker}'
    
    def invest(self,ticker : str, allocated_funds : float, prices : Dict[str,float]):

        price = prices.get(ticker)

        # Buy assets using allocated funds
        if self.backtest_engine.strategy.allow_fractional_shares:
            units_bought = allocated_funds / price
        else:
            units_bought = allocated_funds // price
        self.holdings[ticker] = self.holdings.get(ticker,0.0) + units_bought
        total_cost = round(units_bought * price,2)
            
        # Round units to 4 DP 
        self.holdings[ticker] = round_down(self.holdings[ticker],4)

        # Update cash balance
        self.cash -= total_cost

    def add_cash(self, amount: float):
        """
        Add a specified amount to the holdings cash balance.

        Args:
            amount (float): The amount of cash to add. Must be greater than 0.

        Raises:
            ValueError: If the amount is not positive.
        """
        if amount <= 0:
            raise ValueError("Invalid amount : amount to add to cash balance must be greater than zero")
        self.cash += amount
    
    def add_dividends(self, dividend_per_unit: Dict[str, float], holdings: Dict[str, float]) :
        """
        Record dividend payouts for each ticker held in the portfolio.

        For each ticker in holdings, this method calculates the total dividend income 
        by multiplying the dividend per unit (if any) with the number of units held, 
        and stores the results as a list of dictionaries in `self.dividends`.

        Args:
            dividend_per_unit (Dict[str, float]): Mapping from ticker symbol to dividend per unit.
            holdings (Dict[str, float]): Mapping from ticker symbol to number of units held.
        """
        dividends = []
        tickers = holdings.keys()
        for ticker in tickers:
            div_per_unit = dividend_per_unit.get(ticker,0.0)
            units_held = holdings.get(ticker,0.0)
            if div_per_unit and div_per_unit > 0 and units_held > 0:
                dividends.append({
                    'ticker': ticker,
                    'dividend_per_unit': div_per_unit,
                    'total_dividend': round(div_per_unit * units_held,2)
                })
        self.dividends = dividends

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
