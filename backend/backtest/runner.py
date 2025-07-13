import polars as pl
from datetime import date
from typing import Type
from backend.config import get_result_base_path, get_export_base_path
from backend.models import TargetPortfolio, BacktestConfig
from backend.enums import BacktestMode
from backend.backtest.engine import BaseBacktest,BasicBacktest,RealisticBacktest
from backend.backtest.analysers import BaseAnalyser, RealisticAnalyser
from backend.backtest.new_exporter import Exporter
from backend.backtest.report_generator import ReportGenerator
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
        raw_result_exporter = Exporter(get_result_base_path(), self.timestamp)
        formatted_exporter = Exporter(get_export_base_path(), self.timestamp)
        

        if engine_class is None:
            raise ValueError(f"Unsupported backtest mode: {self.config.mode}")

        backtest = engine_class(
            self.config,
            self.backtest_data,
        )

        result = backtest.run()


        # export raw results
        raw_result_exporter.save_dataframe_to_csv(result.cash,'cash_history')
        raw_result_exporter.save_dataframe_to_csv(result.holdings,'cash_history')

        analyser = analyser_class(result)

        daily_summary_report = ReportGenerator(analyser.generate_daily_summary())
        formatted_exporter.save_report_to_csv(daily_summary_report)
        
        daily_holdings_report = ReportGenerator(analyser.generate_holdings_summary())
        formatted_exporter.save_report_to_csv(daily_holdings_report)
    



    