from backend.models import RealisticBacktestResult
from backend.backtest.export_handlers import BaseResultExportHandler
from backend.backtest.exporter import Exporter
from backend.backtest.analysers import RealisticAnalyser
from backend.backtest.report_generator import ReportGenerator


class RealisticResultExportHandler(BaseResultExportHandler):
    def __init__(self, result : RealisticBacktestResult, exporter: Exporter, analyser : RealisticAnalyser, flat_config_dict : dict[str, str]):
        self.result = result
        self.analyser = analyser
        self.exporter = exporter
        self.flat_config_dict = flat_config_dict


    def export_raw(self) -> None:
        super().export_raw()
        self.exporter.save_dataframe_to_csv(self.result.dividends,'dividends')
        self.exporter.save_dataframe_to_csv(self.result.orders,'orders')

    
    def export_reports(self) -> None:
        super().export_reports()
        
        daily_summary_report = ReportGenerator.generate_csv(self.analyser.generate_daily_summary(),self.flat_config_dict)
        self.exporter.save_report_to_csv(daily_summary_report, 'daily_summary')
        
        daily_holdings_report = ReportGenerator.generate_csv(self.analyser.generate_holdings_summary(),self.flat_config_dict)
        self.exporter.save_report_to_csv(daily_holdings_report, 'holdings_summary')
    

