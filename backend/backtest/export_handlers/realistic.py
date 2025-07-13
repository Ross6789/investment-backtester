from backend.models import RealisticBacktestResult
from backend.backtest.export_handlers import BaseResultExportHandler
from backend.backtest.exporter import Exporter
from backend.backtest.analysers import RealisticAnalyser
from backend.backtest.report_generator import ReportGenerator

class RealisticResultExportHandler(BaseResultExportHandler):
    """
    Handles exporting of raw data and reports specific to the Realistic backtest mode.

    Extends the base export handler by adding exports for additional dataframes such as
    dividends and orders, as well as generating and exporting detailed summary reports.

    Attributes:
        result (RealisticBacktestResult): The full results of the realistic backtest.
        exporter (Exporter): Responsible for saving dataframes and reports to files.
        analyser (RealisticAnalyser): Provides analytical summaries of the backtest results.
        flat_config_dict (dict[str, str]): Flattened configuration dictionary used as metadata in reports.
    """

    def __init__(self, result : RealisticBacktestResult, exporter: Exporter, analyser : RealisticAnalyser, flat_config_dict : dict[str, str]):
        """
        Initializes the export handler with results, analyser, exporter, and config metadata.

        Args:
            result: RealisticBacktestResult containing all raw backtest data.
            exporter: Exporter instance to save files.
            analyser: RealisticAnalyser to generate analytical reports.
            flat_config_dict: Dictionary of flattened config parameters for report comments.
        """
        self.result = result
        self.analyser = analyser
        self.exporter = exporter
        self.flat_config_dict = flat_config_dict


    def export_raw(self) -> None:
        """
        Export raw dataframes to CSV files.

        Calls the base export method to export common raw data, then exports
        additional realistic-mode-specific dataframes such as dividends and orders.
        """
        super().export_raw()
        self.exporter.save_dataframe_to_csv(self.result.dividends,'dividends')
        self.exporter.save_dataframe_to_csv(self.result.orders,'orders')

    
    def export_reports(self) -> None:
        """
        Export analytical reports to CSV files.

        Calls the base report export method to save common summary reports, then
        generates and saves additional detailed reports including dividend summaries,
        yearly pivoted dividend summaries, order summaries, and yearly pivoted order summaries.

        Each report includes configuration metadata formatted as CSV comments.
        """
        super().export_reports()

        dividend_summary_report = ReportGenerator.generate_csv(self.analyser.generate_dividend_summary(),self.flat_config_dict)
        self.exporter.save_report_to_csv(dividend_summary_report, 'dividends_summary')

        dividend_yearly_report = ReportGenerator.generate_csv(self.analyser.generate_pivoted_yearly_dividend_summary(),self.flat_config_dict)
        self.exporter.save_report_to_csv(dividend_yearly_report, 'dividends_yearly')
        
        order_summary_report = ReportGenerator.generate_csv(self.analyser.generate_order_summary(),self.flat_config_dict)
        self.exporter.save_report_to_csv(order_summary_report, 'orders_summary')

        order_yearly_report = ReportGenerator.generate_csv(self.analyser.generate_pivoted_yearly_order_summary(),self.flat_config_dict)
        self.exporter.save_report_to_csv(order_yearly_report, 'orders_yearly')
    

