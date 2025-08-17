from backend.core.models import BacktestConfig
import polars as pl
from pathlib import Path
from backend.utils.saving import generate_timestamp
from backend.backtest.factory import BacktestFactory
from backend.backtest.benchmark_simulator import BenchmarkSimulator
from backend.backtest.exporter import Exporter
from backend.utils.saving import save_report_temporarily

class BacktestRunner:


    def __init__(self, config: BacktestConfig, filtered_price_data: pl.DataFrame, filtered_benchmark_data: pl.DataFrame, export_excel : bool, dev_run: bool = False,  base_save_path: Path | None = None):
            """
            Initialize a BacktestRunner instance.

            Args:
                config (BacktestConfig): Backtest configuration, including strategy parameters and run mode.
                filtered_price_data (pl.DataFrame): Historical price and dividend data, filtered for the selected assets and date range.
                filtered_benchmark_data (pl.DataFrame): Benchmark price data, filtered for the selected benchmarks and date range.
                export_excel (bool): Whether to generate an Excel report for the run.
                dev_run (bool, optional): If True, exports all intermediate backtest data for debugging/development. Defaults to False.
                base_save_path (Path | None, optional): Base directory path for saving outputs. Defaults to None.
            """
            self.config = config
            self.backtest_data = filtered_price_data
            self.benchmark_data = filtered_benchmark_data
            self.export_excel = export_excel
            self.dev_run = dev_run
            self.base_save_path = base_save_path
            self.timestamp = generate_timestamp()

    def run(self) -> dict:
        """
        Run the complete backtest process.

        Workflow:
            1. Instantiate and execute the backtest engine based on the configured mode.
            2. Analyse the backtest results using the appropriate analyser.
            3. Run benchmark simulations for comparison.
            4. Export results:
                - If `dev_run=True`, export all raw outputs (calendar, holdings, balances, etc.).
                - If `export_excel=True`, prepare and save an Excel report (temporarily on server).
            5. Combine portfolio analysis results with benchmark chart data.

        Returns:
            tuple:
                dict: Combined backtest analysis results including benchmark chart data.
                Path | None: Path to the temporary Excel file if `export_excel=True`, otherwise None.
        """
        temp_excel_path = None
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
        benchmark_chart_data = BenchmarkSimulator.run(self.config, self.benchmark_data)

        #Combine potfolio analysis with benchamark chart data
        combined_results = analysis_results.copy()
        combined_results["charts"]["benchmark"] = benchmark_chart_data

        if self.export_excel or self.dev_run:
            # Setup exporter and result handler
            exporter = Exporter(self.base_save_path, self.timestamp)   
            result_export_handler = BacktestFactory.get_result_export_handler(mode,result,combined_results,exporter,analyser,self.config.to_flat_dict())

            # If development run : Export ALL run data to specified local path.
            if self.dev_run:
                result_export_handler.export_all() #Exports raw results file as csv ie. master calender, holdings, cashbalance, aswell as combined excel report

            # If exporting excel : Save the excel file temporarily on server
            if self.export_excel:
                report_sheets = result_export_handler._prepare_report_sheets_for_export()
                temp_excel_path = save_report_temporarily(report_sheets)

        return combined_results, temp_excel_path
        



    