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
        config (BacktestConfig): Strategy and investment settings.
        backtest_data (pl.DataFrame): Full dataset of all tickers and dates.
    """
    def __init__(self, config: BacktestConfig, backtest_data: pl.DataFrame):
            self.config = config
            self.backtest_data = backtest_data


    def run(self):
        """
        Instantiates and runs the selected backtest mode and returns analysed results.
        """
        backtest_class = self.BACKTEST_CLASS_MAP.get(self.config.mode)

        if backtest_class is None:
            raise ValueError(f"Unsupported backtest mode: {self.config.mode}")

        backtest = backtest_class(
            self.config,
            self.backtest_data,
        )

        result = backtest.run()
        return result

    