from abc import ABC
from backend.models import BacktestResult
from backend.backtest.exporter import Exporter
from backend.backtest.analysers import BaseAnalyser
from backend.backtest.report_generator import ReportGenerator


class BaseResultExportHandler(ABC):
    def __init__(self, result : BacktestResult, exporter: Exporter, analyser : BaseAnalyser, flat_config_dict : dict[str, str]):
        self.result = result
        self.analyser = analyser
        self.exporter = exporter
        self.flat_config_dict = flat_config_dict


    def export_all(self) -> None:
        self.export_raw()
        self.export_reports()

    
    def export_raw(self) -> None:
        self.exporter.save_dataframe_to_csv(self.result.data,'data')
        self.exporter.save_dataframe_to_csv(self.result.calendar,'calendar')
        self.exporter.save_dataframe_to_csv(self.result.cash,'cash_history')
        self.exporter.save_dataframe_to_csv(self.result.holdings,'holdings_history')

    
    def export_reports(self) -> None:

        daily_summary_report = ReportGenerator.generate_csv(self.analyser.generate_daily_summary(),self.flat_config_dict)
        self.exporter.save_report_to_csv(daily_summary_report, 'daily_summary')
        
        daily_holdings_report = ReportGenerator.generate_csv(self.analyser.generate_holdings_summary(),self.flat_config_dict)
        self.exporter.save_report_to_csv(daily_holdings_report, 'holdings_summary')
    


    