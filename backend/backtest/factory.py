import polars as pl
from backend.core.models import BacktestConfig, BacktestResult
from backend.core.enums import BacktestMode
from backend.backtest.engines import BaseEngine,BasicEngine,RealisticEngine
from backend.backtest.analysers import BaseAnalyser, RealisticAnalyser
from backend.backtest.exporter import Exporter
from backend.backtest.export_handlers import BaseResultExportHandler, RealisticResultExportHandler

class BacktestFactory:
    """
    Factory class for instantiating backtest components based on the selected BacktestMode.

    This class provides static methods to retrieve the appropriate engine, analyser,
    and export handler implementations for different backtest modes (e.g., BASIC, REALISTIC).
    """
    
    @staticmethod
    def get_engine(mode: BacktestMode, config: BacktestConfig, backtest_data: pl.DataFrame) -> BaseEngine:
        """
        Returns the appropriate backtest engine instance for the given mode.

        Args:
            mode (BacktestMode): The selected backtest mode (BASIC or REALISTIC).
            config (BacktestConfig): Backtest configuration object.
            backtest_data (pl.DataFrame): The full dataset used for the backtest.

        Returns:
            BaseEngine: An instance of the corresponding backtest engine.

        Raises:
            ValueError: If the mode is not supported.
        """
        match mode:
            case BacktestMode.BASIC:
                return BasicEngine(config,backtest_data)
            case BacktestMode.REALISTIC:
                return RealisticEngine(config,backtest_data)
            case _:
                raise ValueError(f"No backtest engine defined for mode: {mode}")


    @staticmethod
    def get_analyser(mode: BacktestMode, backtest_results : BacktestResult) -> BaseAnalyser:
        """
        Returns the appropriate analyser for the given backtest mode.

        Args:
            mode (BacktestMode): The selected backtest mode.
            backtest_results (BacktestResult): The results produced by the backtest engine.

        Returns:
            BaseAnalyser: An instance of the corresponding analyser.

        Raises:
            ValueError: If the mode is not supported.
        """
        match mode:
            case BacktestMode.BASIC:
                return BaseAnalyser(backtest_results)
            case BacktestMode.REALISTIC:
                return RealisticAnalyser(backtest_results)
            case _:
                raise ValueError(f"No backtest analyser defined for mode: {mode}")


    @staticmethod
    def get_result_export_handler(mode: BacktestMode, raw_result : BacktestResult,analysed_result: dict, exporter: Exporter, analyser : BaseAnalyser, flat_config_dict : dict[str, str]) -> BaseResultExportHandler:
        """
        Returns the appropriate result export handler for saving raw results and summaries.

        Args:
            mode (BacktestMode): The selected backtest mode.
            result (BacktestResult): The raw result data from the backtest.
            analysed_result (dict): The analysed results used to populate the dashboard
            exporter (Exporter): The exporter instance used for file saving.
            analyser (BaseAnalyser): The analyser used to generate reports.
            flat_config_dict (dict[str, str]): Flattened configuration dictionary for metadata.

        Returns:
            BaseResultExportHandler: An instance of the corresponding export handler.

        Raises:
            ValueError: If the mode is not supported.
        """
        match mode:
            case BacktestMode.BASIC:
                return BaseResultExportHandler(raw_result,analysed_result,exporter,analyser,flat_config_dict)
            case BacktestMode.REALISTIC:
                return RealisticResultExportHandler(raw_result,analysed_result,exporter,analyser,flat_config_dict)
            case _:
                raise ValueError(f"No backtest result export handler defined for mode: {mode}")