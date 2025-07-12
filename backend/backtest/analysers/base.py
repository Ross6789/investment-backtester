from abc import ABC, abstractmethod
from datetime import date
import polars as pl
from backend.models import TargetPortfolio, BacktestConfig
from backend.utils import generate_recurring_dates

class BaseAnalyser(ABC):
    def __init__(self, start_date: date, end_date: date, backtest_data: pl.DataFrame, target_portfolio: TargetPortfolio ,config: BacktestConfig):
        self.start_date = start_date
        self.end_date = end_date
        self.backtest_data = backtest_data
        self.target_portfolio = target_portfolio
        self.config = config
