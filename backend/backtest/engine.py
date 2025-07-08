import polars as pl
from datetime import date
from backend.backtest.portfolios.base import BasePortfolio
from backend.models import TargetPortfolio, BacktestConfig
from backend.enums import OrderSide, RebalanceFrequency, BacktestMode, ReinvestmentFrequency
from dateutil.relativedelta import relativedelta
from backend.backtest.modes.basic import BasicBacktest, RealisticBacktest
from backend.backtest.portfolios.base import BasePortfolio

class BacktestEngine:
    """
    Runs portfolio backtests over a specified date range and dataset.

    Attributes:
        start_date (date): Start date of the backtest.
        end_date (date): End date of the backtest.
        backtest_data (pl.DataFrame): Full dataset of all tickers and dates.
        target_portfolio (TargetPortfolio): Desired portfolio allocation.
        config (BacktestConfig): Strategy and investment settings.
    """
    def __init__(self, start_date: date, end_date: date, backtest_data: pl.DataFrame, target_portfolio: TargetPortfolio ,config: BacktestConfig):
            self.start_date = start_date
            self.end_date = end_date
            self.backtest_data = backtest_data
            self.target_portfolio = target_portfolio
            self.config = config

    def run(self):

        backtest = _find_backtest_class()

        run_data = backtest.run()
        result_data = backtest.analyse(run_data)


    def _find_backtest_class(self):
        match self.config.mode:
            case BacktestMode.BASIC:
                return BasicBacktest()
            case BacktestMode.REALISTIC:
                return RealisticBacktest()
            case _:
                raise ValueError(f"Unsupported backtest mode: {self.config.mode}")
    