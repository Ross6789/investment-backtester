from abc import ABC, abstractmethod
from datetime import date
import polars as pl
from backend.backtest.portfolio import Portfolio
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
            self.portfolio = Portfolio(self)
            self._generate_master_calendar()
            self._generate_ticker_active_dates()

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

        # Join active and trading tickers
        master_calendar = active_tickers_calendar.join(trading_tickers_calendar, on='date',how='left')

        # Convert to dictionary for quick lookups
        self.master_calendar_dict = {
            row["date"]: {
                "active_tickers": set(row["active_tickers"]),
                "trading_tickers": set(row["trading_tickers"])
            }
            for row in master_calendar.iter_rows(named=True)
        }
    

    def _generate_recurring_cashflow_dates(self, start_date: date, end_date: date, frequency: ReinvestmentFrequency) -> set[date]:
        """
        Generate a set of recurring cashflow dates within a date range based on the given frequency.

        Args:
            start_date (date): The starting date for generating cashflow dates (inclusive).
            end_date (date): The ending date for generating cashflow dates (inclusive).
            frequency (ReinvestmentFrequency): Frequency of the recurring cashflows.

        Returns:
            set[date]: A set of dates on which recurring cashflows occur.

        Raises:
            ValueError: If the provided frequency is invalid.
        """
        dates = set()
        cashflow_date = start_date
        while cashflow_date <= end_date:
            dates.add(cashflow_date)
            match frequency:
                case ReinvestmentFrequency.DAILY:
                    cashflow_date += relativedelta(days=1)
                case ReinvestmentFrequency.WEEKLY:
                    cashflow_date += relativedelta(weeks=1)
                case ReinvestmentFrequency.MONTHLY:
                    cashflow_date += relativedelta(months=1)
                case ReinvestmentFrequency.QUARTERLY:
                    cashflow_date += relativedelta(months=3)
                case ReinvestmentFrequency.YEARLY:
                    cashflow_date += relativedelta(years=1)
                case _:
                    raise ValueError('Invalid recurring investment frequency')
        return set(dates)
