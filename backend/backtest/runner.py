import polars as pl
from datetime import date
from typing import Type
from backend.config import get_backtest_run_base_path
from backend.models import TargetPortfolio, BacktestConfig
from backend.enums import BacktestMode
from backend.backtest.engine import BaseBacktest,BasicBacktest,RealisticBacktest
from backend.backtest.analysers import BaseAnalyser, RealisticAnalyser
from backend.backtest.exporter import Exporter
from backend.backtest.report_generator import ReportGenerator
from backend.backtest.export_handlers import BaseResultExportHandler
from backend.utils import generate_timestamp
from pathlib import Path


class BacktestRunner:

    BACKTEST_ENGINE_CLASS_MAP: dict[BacktestMode, Type[BaseBacktest]] = {
        BacktestMode.BASIC: BasicBacktest,
        BacktestMode.REALISTIC: RealisticBacktest,
    }

    BACKTEST_ANALYSER_CLASS_MAP: dict[BacktestMode, Type[BaseAnalyser]] = {
        BacktestMode.BASIC: BaseAnalyser,
        BacktestMode.REALISTIC: RealisticAnalyser,
    }

    """
    Runs portfolio backtests over a specified date range and dataset.

    Attributes:
        config (BacktestConfig): Strategy and investment settings.
        backtest_data (pl.DataFrame): Full dataset of all tickers and dates.
        base)save_path (Path): Base save path where raw results and summaries will be exported
    """
    def __init__(self, config: BacktestConfig, backtest_data: pl.DataFrame, base_save_path: Path):
            self.config = config
            self.backtest_data = backtest_data
            self.base_save_path = base_save_path
            self.timestamp = generate_timestamp()


    def run(self):
        """
        Instantiates and runs the selected backtest mode and returns analysed results.
        """
        engine_class = self.BACKTEST_ENGINE_CLASS_MAP.get(self.config.mode)
        analyser_class = self.BACKTEST_ANALYSER_CLASS_MAP.get(self.config.mode)
        exporter = Exporter(self.base_save_path, self.timestamp)

        

        if engine_class is None:
            raise ValueError(f"Unsupported backtest mode: {self.config.mode}")

        backtest = engine_class(
            self.config,
            self.backtest_data,
        )
        flat_config_dict = backtest.config.to_flat_dict()

        result = backtest.run()

        analyser = analyser_class(result)

        result_exporter = BaseResultExportHandler(result,exporter,analyser,flat_config_dict)

        result_exporter.export_all()





    