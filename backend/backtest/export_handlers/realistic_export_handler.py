from backend.core.models import RealisticBacktestResult
from backend.backtest.export_handlers import BaseResultExportHandler
from backend.backtest.exporter import Exporter
from backend.backtest.analysers import RealisticAnalyser
from backend.backtest.report_generator import ReportGenerator
from backend.core.models import RoundingConfig

import polars as pl

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

    def __init__(self, raw_result : RealisticBacktestResult, analysed_result : dict,exporter: Exporter, analyser : RealisticAnalyser, flat_config_dict : dict[str, str]):
        """
        Initializes the export handler with results, analyser, exporter, and config metadata.

        Args:
            raw_result: RealisticBacktestResult containing all raw backtest data.
            analysed_results: The fully analysed results of teh backtest run
            exporter: Exporter instance to save files.
            analyser: RealisticAnalyser to generate analytical reports.
            flat_config_dict: Dictionary of flattened config parameters for report comments.
        """
        self.raw_result = raw_result
        self.analysed_result = analysed_result
        self.analyser = analyser
        self.exporter = exporter
        self.flat_config_dict = flat_config_dict


    def export_raw_csv(self) -> None:
        """
        Export raw dataframes to CSV files.

        Calls the base export method to export common raw data, then exports
        additional realistic-mode-specific dataframes such as dividends and orders.
        """
        super().export_raw_csv()
        self.exporter.save_dataframe_to_csv(self.raw_result.dividends,'dividends')
        self.exporter.save_dataframe_to_csv(self.raw_result.orders,'orders')

    
    # def export_reports(self) -> None:
    #     """
    #     Export analytical reports to CSV files.

    #     Calls the base report export method to save common summary reports, then
    #     generates and saves additional detailed reports including dividend summaries,
    #     yearly pivoted dividend summaries, order summaries, and yearly pivoted order summaries.

    #     Each report includes configuration metadata formatted as CSV comments.
    #     """
    #     super().export_reports()

    #     dividend_summary_report = ReportGenerator.generate_formatted_report(self.analyser.generate_dividend_summary(),self.flat_config_dict)
    #     self.exporter.save_report_to_csv(dividend_summary_report, 'dividends_summary')

    #     dividend_yearly_report = ReportGenerator.generate_formatted_report(self.analyser.generate_pivoted_yearly_dividend_summary(),self.flat_config_dict)
    #     self.exporter.save_report_to_csv(dividend_yearly_report, 'dividends_yearly')
        
    #     order_summary_report = ReportGenerator.generate_formatted_report(self.analyser.generate_order_summary(),self.flat_config_dict)
    #     self.exporter.save_report_to_csv(order_summary_report, 'orders_summary')

    #     order_yearly_report = ReportGenerator.generate_formatted_report(self.analyser.generate_pivoted_yearly_order_summary(),self.flat_config_dict)
    #     self.exporter.save_report_to_csv(order_yearly_report, 'orders_yearly')
    

    def _prepare_report_sheets_for_export(self) -> dict[str, pl.DataFrame]:
        """Prepare extended backtest reports for export, including realistic-mode reports.

        This method extends the base report preparation from the superclass by adding:
        - Dividend summaries (both regular and yearly pivoted)
        - Order summaries (both regular and yearly pivoted)

        Rounding configuration will use defaults from the constants file if not specified.

        Returns:
            dict[str, pl.DataFrame]: Mapping of Excel sheet names to Polars DataFrames.
        """
        # Prepare base reports
        report_sheets = super()._prepare_report_sheets_for_export() 

        # ---- Rounding configuration (will use defaults declared in constant file if no rounding config passed in) ----
        rounding = RoundingConfig(price_precision=2,currency_precision=2,percentage_precision=1,general_precision=1)

        # ---- Realistic mode reports ----
        # Dividend summary
        dividend_summary_pl = self.analyser.generate_dividend_summary()
        dividend_report= ReportGenerator.generate_formatted_report(df=dividend_summary_pl,rounding_config=rounding)
        report_sheets["Dividends"] = dividend_report

        # Dividend yearly summary
        dividend_yearly_summary_pl = self.analyser.generate_pivoted_yearly_dividend_summary()
        dividend_yearly_report= ReportGenerator.generate_formatted_report(df=dividend_yearly_summary_pl,rounding_config=rounding)
        report_sheets["Dividends (Yearly)"] = dividend_yearly_report

        # Order summary
        order_summary_pl = self.analyser.generate_order_summary()
        order_report= ReportGenerator.generate_formatted_report(df=order_summary_pl,rounding_config=rounding)
        report_sheets["Orders"] = order_report

        # Order yearly summary
        order_yearly_summary_pl = self.analyser.generate_pivoted_yearly_order_summary()
        order_yearly_report= ReportGenerator.generate_formatted_report(df=order_yearly_summary_pl,rounding_config=rounding)
        report_sheets["Orders (Yearly)"] = order_yearly_report

        return report_sheets