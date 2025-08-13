from backend.core.models import BacktestConfig
import polars as pl
from pathlib import Path
from backend.utils.saving import generate_timestamp
from backend.backtest.factory import BacktestFactory
from backend.backtest.benchmark_simulator import BenchmarkSimulator
from backend.backtest.exporter import Exporter

class BacktestRunner:
    """
    Coordinates the execution of portfolio backtests, analysis, and result exporting.

    This class serves as the main orchestrator for running backtests according to
    the provided configuration and dataset. It selects and instantiates the appropriate
    engine, analyser, and export handler based on the backtest mode, executes the backtest,
    generates analyses, and exports all results and reports to the designated output folder.

    Attributes:
        config (BacktestConfig): Configuration detailing strategy, mode, and parameters.
        backtest_data (pl.DataFrame): The dataset containing all relevant financial data.
        base_save_path (Path): Directory where all backtest outputs will be saved.
        timestamp (str): Timestamp used to create unique output folders per run.
    """

    def __init__(self, config: BacktestConfig, backtest_data: pl.DataFrame, benchmark_data: pl.DataFrame, benchmark_metadata_path: Path, base_save_path: Path):
            """
            Initializes the BacktestRunner with configuration, data, and save path.

            Args:
                config: BacktestConfig object containing strategy and mode.
                backtest_data: Polars DataFrame with all backtest price and dividend data.
                benchmark_data: Polars DataFrame with all benchmark data.
                benchmark_metadata_path: Path to benchmark metadata
                base_save_path: Base directory path to save outputs.
            """
            self.config = config
            self.backtest_data = backtest_data
            self.benchmark_data = benchmark_data
            self.benchmark_metadata_path = benchmark_metadata_path
            self.base_save_path = base_save_path
            self.timestamp = generate_timestamp()

    def run(self) -> dict:
        """
        Runs the backtest process:
        - Instantiates engine, analyser, exporter, and export handler based on mode.
        - Executes the backtest engine.
        - Generates and exports all results and reports.
        """

        print("Running backtest...")
        mode = self.config.mode
        
        # Create engine and run backtest
        print("Starting engine...")
        engine = BacktestFactory.get_engine(mode,self.config,self.backtest_data)
        result = engine.run()
        print("Engine finished! Starting analysis and export...")

        # Create analyser based on mode and backtest results
        analyser = BacktestFactory.get_analyser(mode,result)
        analysis_results = analyser.run()
        # print(metrics)

        # Perform skeleton benchmark simulations
        benchmark_chart_data = BenchmarkSimulator.run(self.config, self.benchmark_data, self.benchmark_metadata_path)

        # Setup exporter for saving results
        exporter = Exporter(self.base_save_path, self.timestamp)

        # # Get export handler and export all results/reports
        result_export_handler = BacktestFactory.get_result_export_handler(mode,result,exporter,analyser,self.config.to_flat_dict())
        result_export_handler.export_all()
        print("Export finished!")

        #Combine potfolio analysis with benchamark chart data
        combined_results = analysis_results.copy()
        combined_results["charts"]["benchmark"] = benchmark_chart_data
        return combined_results
        



    