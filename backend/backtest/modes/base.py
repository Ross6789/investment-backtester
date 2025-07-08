from abc import ABC, abstractmethod
from datetime import date
import polars as pl
from backend.backtest.portfolios.base import BasePortfolio
from backend.models import TargetPortfolio, BacktestConfig
from backend.enums import ReinvestmentFrequency
from dateutil.relativedelta import relativedelta

class BaseBacktest(ABC):
    def __init__(self, start_date: date, end_date: date, backtest_data: pl.DataFrame, target_portfolio: TargetPortfolio ,config: BacktestConfig):
            self.start_date = start_date
            self.end_date = end_date
            self.backtest_data = backtest_data
            self.target_portfolio = target_portfolio
            self.config = config

            # Generate master calendar
            self._generate_master_calendar()
    
    @abstractmethod
    def _should_rebalance(self):
        pass

    @abstractmethod
    def rebalance(self):
        pass

    @abstractmethod
    def run(self):
        pass

    @abstractmethod
    def analyse(self):
        pass

    # --- Data Generation & Loading ---

    def _generate_ticker_active_dates(self) -> pl.DataFrame:
        """
        Determine the active date range for each ticker in the backtest data.

        Groups data by ticker and calculates the earliest and latest dates 
        each ticker appears in the dataset.

        Returns:
            pl.DataFrame with columns:
                - 'ticker': The asset ticker symbol.
                - 'first_active_date': The first date the ticker appears.
                - 'last_active_date': The last date the ticker appears.
        """
        ticker_active_dates = (
            self.backtest_data
            .group_by('ticker')
            .agg([
                pl.col('date').min().alias('first_active_date'),
                pl.col('date').max().alias('last_active_date')
            ])
            .sort('ticker')
        )
        return ticker_active_dates


    def _generate_master_calendar(self) -> None:
        """
        Generate a per-date dictionary of active and trading tickers.

        For each date where at least one ticker is active, store:
            {
                date: {
                    "active_tickers": set[str],
                    "trading_tickers": set[str]
                }
            }

        - Active tickers are based on first/last active date ranges.
        - Trading tickers are where 'is_trading_day' is True.
        - Result is saved to `self.master_calendar_dict`.
        """

        date_range = pl.DataFrame(pl.date_range(self.start_date,self.end_date,interval="1d",eager=True))

        ticker_active_dates = self._generate_ticker_active_dates()
        
        # Generate all dates where each ticker was active
        active_tickers = (
            self.backtest_data.join(
                ticker_active_dates,
                how="cross"
            )
            .filter(
                (pl.col("date") >= pl.col("first_active_date")) &
                (pl.col("date") <= pl.col("last_active_date"))
            )
            .select(["date", "ticker"])
        )
        
        # Group active tickers by date
        active_tickers_calendar = (
            active_tickers
            .group_by('date')
            .agg([
                pl.col('ticker').unique().sort().alias('active_tickers')
            ])
            .sort('date')
        )

        # Group trading tickers by date
        trading_tickers_calendar = (
            self.backtest_data
            .group_by('date')
            .agg([
                pl.col('ticker').filter(pl.col('is_trading_day')==True).unique().sort().alias('trading_tickers')
            ])
            .sort('date')
        )

        # Join active and trading tickers to full date range and fill nulls with empty lists
        master_calendar = (
            date_range
            .join(active_tickers_calendar,on='date',how='left')
            .join(trading_tickers_calendar, on='date',how='left')
            .fill_null([])
        )

        # Convert to dictionary for quick lookups
        master_calendar_dict = {
            row["date"]: {
                "active_tickers": set(row["active_tickers"]),
                "trading_tickers": set(row["trading_tickers"])
            }
            for row in master_calendar.iter_rows(named=True)
        }

        # Save internally
        self.master_calendar_df =  master_calendar # For efficeint scan and fitlering
        self.master_calendar_dict = master_calendar_dict # For efficient date lookups


    # --- Ticker Lookup & Filtering ---

    def _find_active_tickers(self, date) -> set[str]:
        """
        Find all tickers that are active on a given date using the master calendar.

        Args:
            date (date): The date to check.

        Returns:
            Set[str]: A set of ticker symbols active on the specified date.
        """
        active_tickers = self.master_calendar_dict.get(date, {}).get("active_tickers", set())
                
        return active_tickers

    
    def _all_active_tickers_trading(self, date: date) -> bool:
        """
        Check whether all active tickers on a given date are also trading.

        Args:
            date (date): The date to check.

        Returns:
            bool: True if all active tickers are trading and at least one ticker is active,
                False otherwise.
        """
        day_info = self.master_calendar_dict.get(date, {})
        active_tickers = day_info.get("active_tickers", set())
        trading_tickers = day_info.get("trading_tickers", set())

        return active_tickers == trading_tickers and len(active_tickers) > 0

    # --- Portfolio Normalization ---

    def _normalize_portfolio_targets(self, date: date) -> dict[str,float]:
        """
        Normalize target portfolio weights for tickers active on the given date.

        Filters the target portfolio weights to include only tickers active on the specified date,
        then normalizes these weights so their sum equals 1.

        Args:
            date (date): The date to determine which tickers are active.

        Returns:
            dict[str, float]: A dictionary mapping active tickers to their normalized target weights.
        """
        active_tickers = self._find_active_tickers(date)
        filtered_weights = {ticker: weight for ticker, weight in self.target_portfolio.weights.items() if ticker in active_tickers}

        total_weight = sum(filtered_weights.values())

        normalized_weights = {ticker : weight / total_weight for ticker, weight in filtered_weights.items()}
        
        return normalized_weights
    

    def _get_ticker_allocations_by_target(self, normalized_weights: dict[str, float], total_value_to_allocate: float) -> dict[str, float]:
        """
        Calculate the dollar allocation for each ticker based on target normalized weights and total allocation amount.

        Args:
            normalized_weights (dict[str, float]): A dictionary mapping ticker symbols to their target portfolio weights (normalized to sum to 1).
            total_value_to_allocate (float): The total dollar amount available to allocate among the tickers.

        Returns:
            dict[str, float]: A dictionary mapping each ticker to the dollar amount allocated based on its target weight.
        """
        return {ticker: weight*total_value_to_allocate for ticker, weight in normalized_weights.items()}

  
    # --- Price retrieval ---

    def _get_prices_on_date(self, date: date) -> dict[str,float]:
        """
        Retrieve prices for all tickers on the given date.

        Args:
            date (date): The date to fetch prices for.

        Returns:
            dict[str, float]: Mapping of ticker symbols to their prices on the date.
        """
        prices_df = (
            self.backtest_data
            .filter(pl.col('date')==date)
            .select(['ticker','price'])
        )
        return dict(zip(prices_df['ticker'], prices_df['price']))
