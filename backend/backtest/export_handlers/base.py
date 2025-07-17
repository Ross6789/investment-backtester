from abc import ABC
from backend.core.models import BacktestResult
from backend.backtest.exporter import Exporter
from backend.backtest.analysers import BaseAnalyser
from backend.backtest.report_generator import ReportGenerator
from backend.utils.reporting import generate_suffixed_col_names

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

    def __init__(self, result : BacktestResult, exporter: Exporter, analyser : BaseAnalyser, flat_config_dict : dict[str, str]):
        """
        Initializes the export handler with backtest results, analyser, exporter, and configuration metadata.

        Args:
            result: The BacktestResult object containing raw backtest data.
            exporter: An Exporter instance to handle saving files.
            analyser: A BaseAnalyser instance for generating reports.
            flat_config_dict: A dictionary of flattened configuration parameters for report metadata.
        """
        self.result = result
        self.analyser = analyser
        self.exporter = exporter
        self.flat_config_dict = flat_config_dict


    def export_all(self) -> None:
        """
        Export all relevant backtest data and reports.
        """
        self.export_raw()
        self.export_reports()

    
    def export_raw(self) -> None:
        """
        Export raw dataframes from the backtest result to CSV files.

        Exports the core raw dataframes such as the main data, calendar, cash history,
        and holdings history to separate CSV files.
        """
        self.exporter.save_dataframe_to_csv(self.result.data,'data')
        self.exporter.save_dataframe_to_csv(self.result.calendar,'calendar')
        self.exporter.save_dataframe_to_csv(self.result.cash,'cash_history')
        self.exporter.save_dataframe_to_csv(self.result.holdings,'holdings_history')

    
    def export_reports(self) -> None:
        """
        Generate and export summary reports to CSV files.

        Uses the analyser to generate daily summary and holdings summary reports,
        then saves them with configuration metadata as comments.
        """
        daily_summary_report = ReportGenerator.generate_csv(df=self.analyser.generate_daily_summary(),metadata=self.flat_config_dict,percentify_cols=['daily_return'])
        self.exporter.save_report_to_csv(daily_summary_report, 'daily_summary')
        
        pivoted_precentage_cols = generate_suffixed_col_names(['portfolio_weighting'], self.analyser.tickers) # Find pivoted col names for percentage conversion
        daily_holdings_report = ReportGenerator.generate_csv(df=self.analyser.generate_holdings_summary(),metadata=self.flat_config_dict,percentify_cols=pivoted_precentage_cols)
        self.exporter.save_report_to_csv(daily_holdings_report, 'holdings_summary')
    

