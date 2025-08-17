from abc import ABC
import polars as pl
from backend.core.models import BacktestResult
from backend.backtest.exporter import Exporter
from backend.backtest.analysers import BaseAnalyser
from backend.backtest.report_generator import ReportGenerator
from backend.utils.reporting import generate_suffixed_col_names
from backend.core.models import RoundingConfig

class BaseResultExportHandler(ABC):
    """
    Base class to handle exporting of backtest results including raw data and summary reports.

    This class provides default implementations for exporting raw dataframes and
    generating reports from analysed backtest results. Subclasses can override
    methods to extend or customize export behavior.

    Attributes:
        result (BacktestResult): The raw results from the backtest engine.
        exporter (Exporter): Responsible for saving files to disk.
        analyser (BaseAnalyser): Provides analytical summaries of the backtest results.
        flat_config_dict (dict[str, str]): Flattened configuration dictionary used as metadata in reports.
    """

    def __init__(self, raw_result : BacktestResult, analysed_result : dict, exporter: Exporter, analyser : BaseAnalyser, flat_config_dict : dict[str, str]):
        """
        Initializes the export handler with backtest results, analyser, exporter, and configuration metadata.

        Args:
            raw_result: The BacktestResult object containing raw backtest data.
            analysed_results: The fully analysed results of the backtest run
            exporter: An Exporter instance to handle saving files.
            analyser: A BaseAnalyser instance for generating reports.
            flat_config_dict: A dictionary of flattened configuration parameters for report metadata.
        """
        self.raw_result = raw_result
        self.analysed_result = analysed_result
        self.analyser = analyser
        self.exporter = exporter
        self.flat_config_dict = flat_config_dict


    def export_all(self) -> None:
        """
        Export all relevant backtest data and reports.
        """

        self.export_raw_csv()
        self.export_report_excel()
        self.export_dashboard_json()

    
    def export_raw_csv(self) -> None:
        """
        Export raw dataframes from the backtest result to CSV files.

        Exports the core raw dataframes such as the main data, calendar, cash history,
        and holdings history to separate CSV files.
        """
        self.exporter.save_dataframe_to_csv(self.raw_result.data,'data')
        self.exporter.save_dataframe_to_csv(self.raw_result.calendar,'calendar')
        self.exporter.save_dataframe_to_csv(self.raw_result.cash,'cash_history')
        self.exporter.save_dataframe_to_csv(self.raw_result.holdings,'holdings_history')

    
    # def export_reports(self) -> None:
    #     """
    #     Generate and export summary reports to CSV files.

    #     Uses the analyser to generate daily summary and holdings summary reports, then saves them with configuration metadata as comments.
    #     """
    #     daily_summary_report = ReportGenerator.generate_csv(df=self.analyser.generate_daily_summary(),metadata=self.flat_config_dict,percentify_cols=['net_daily_return'])
    #     self.exporter.save_report_to_csv(daily_summary_report, 'daily_summary')
        
    #     pivoted_precentage_cols = generate_suffixed_col_names(['portfolio_weighting'], self.analyser.tickers) # Find pivoted col names for percentage conversion
    #     daily_holdings_report = ReportGenerator.generate_csv(df=self.analyser.generate_holdings_summary(),metadata=self.flat_config_dict,percentify_cols=pivoted_precentage_cols)
    #     self.exporter.save_report_to_csv(daily_holdings_report, 'holdings_summary')
    

    def export_report_excel(self) -> None:
        """Export all backtest reports to a single multi-sheet Excel workbook.

        Each report is written to its own sheet. The configuration metadata is
        included in the 'Configuration' sheet.
        """
        
        file_name = "backtest_report"
        report_sheets = self._prepare_report_sheets_for_export()
        self.exporter.save_dataframes_to_excel_workbook(report_sheets,file_name)



    def _prepare_report_sheets_for_export(self) -> dict[str, pl.DataFrame]:
        """Prepare all backtest reports as a dictionary of sheet name → DataFrame.

        Returns:
            dict[str, pl.DataFrame]: Mapping of Excel sheet names to Polars DataFrames.
        """
        report_sheets = {}

        # ---- Rounding configuration (will use defaults declared in constant file if no rounding config passed in) ----
        rounding = RoundingConfig(price_precision=2,currency_precision=2,percentage_precision=1,general_precision=1)

        # ---- Configuration settings ----
        # Turn settings dict into vertical dataframe
        config_df = pl.DataFrame({
            "Setting":list(self.flat_config_dict.keys()),
            "Value":list(self.flat_config_dict.values())
        })
        report_sheets["Configuration"] = config_df

        # ---- Base reports ----
        # Daily summary
        daily_summary_pl = self.analyser.generate_daily_summary()
        daily_report= ReportGenerator.generate_formatted_report(df=daily_summary_pl,percentify_cols=['net_daily_return'],rounding_config=rounding)
        report_sheets["Daily Summary"] = daily_report

        # Holdings summary
        pivoted_percentage_cols = generate_suffixed_col_names(['portfolio_weighting'], self.analyser.tickers)
        holdings_summary_pl = self.analyser.generate_holdings_summary()
        holding_report= ReportGenerator.generate_formatted_report(df=holdings_summary_pl,percentify_cols=pivoted_percentage_cols,rounding_config=rounding)
        report_sheets["Holdings"] = holding_report

        return report_sheets


    def export_dashboard_json(self) -> None:
            
            file_name = "dashboard_results"
            self.exporter.save_dashboard_results_to_json(self.analysed_result,file_name)