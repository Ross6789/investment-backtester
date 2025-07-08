from abc import ABC, abstractmethod
from datetime import date
import polars as pl
from backend.backtest.portfolios.base_portfolio import BasePortfolio
from backend.models import TargetPortfolio, BacktestConfig
from backend.enums import ReinvestmentFrequency
from dateutil.relativedelta import relativedelta

class BacktestBase(ABC):
    def __init__(self, start_date: date, end_date: date, backtest_data: pl.DataFrame, target_portfolio: TargetPortfolio ,config: BacktestConfig):
            self.start_date = start_date
            self.end_date = end_date
            self.backtest_data = backtest_data
            self.target_portfolio = target_portfolio
            self.config = config

            # Instantiate portfolio and data
            self.portfolio = BasePortfolio(self)
            self._generate_master_calendar()

    @abstractmethod
    def run(self):
        pass

    @abstractmethod
    def run(self):
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
        self.master_calendar = {
            row["date"]: {
                "active_tickers": set(row["active_tickers"]),
                "trading_tickers": set(row["trading_tickers"])
            }
            for row in master_calendar.iter_rows(named=True)
        }
    
    # --- Ticker Lookup & Filtering ---

    def _find_active_tickers(self, date) -> set[str]:
        """
        Find all tickers that are active on a given date using the master calendar.

        Args:
            date (date): The date to check.

        Returns:
            Set[str]: A set of ticker symbols active on the specified date.
        """
        active_tickers = self.master_calendar.get(date, {}).get("active_tickers", set())
                
        return active_tickers
    
    # def _find_trading_tickers(self, date) -> set[str]:
    #     """
    #     Find all tickers that are trading on a given date using the master calendar.

    #     Args:
    #         date (date): The date to check.

    #     Returns:
    #         Set[str]: A set of ticker symbols trading on the specified date.
    #     """
    #     trading_tickers = self.master_calendar.get(date, {}).get("trading_tickers", set())
        
    #     return trading_tickers
    
    def _all_active_tickers_trading(self, date: date) -> bool:
        """
        Check whether all active tickers on a given date are also trading.

        Args:
            date (date): The date to check.

        Returns:
            bool: True if all active tickers are trading and at least one ticker is active,
                False otherwise.
        """
        day_info = self.master_calendar.get(date, {})
        active_tickers = day_info.get("active_tickers", set())
        trading_tickers = day_info.get("trading_tickers", set())

        return active_tickers == trading_tickers and len(active_tickers) > 0

