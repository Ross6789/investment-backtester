import polars as pl
from datetime import date
from typing import Type
from backend.models import TargetPortfolio, BacktestConfig
from backend.enums import BacktestMode
from backend.backtest.engine import BaseBacktest,BasicBacktest,RealisticBacktest


class BacktestRunner:

    BACKTEST_CLASS_MAP: dict[BacktestMode, Type[BaseBacktest]] = {
        BacktestMode.BASIC: BasicBacktest,
        BacktestMode.REALISTIC: RealisticBacktest,
    }

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
        """
        Instantiates and runs the selected backtest mode and returns analysed results.
        """
        backtest_class = self.BACKTEST_CLASS_MAP.get(self.config.mode)

        if backtest_class is None:
            raise ValueError(f"Unsupported backtest mode: {self.config.mode}")

        backtest = backtest_class(
            self.start_date,
            self.end_date,
            self.backtest_data,
            self.target_portfolio,
            self.config,
        )

        run_data = backtest.run()
        return run_data

    