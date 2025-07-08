from backend.backtest.modes.base import BaseBacktest
from backend.backtest.portfolios.base import BasePortfolio
from datetime import date
import polars as pl
from backend.models import TargetPortfolio, BacktestConfig
from backend.utils import generate_recurring_dates

class BasicBacktest(BaseBacktest):
    def __init__(self, start_date: date, end_date: date, backtest_data: pl.DataFrame, target_portfolio: TargetPortfolio ,config: BacktestConfig):
        
        super().__init__(start_date, end_date, backtest_data, target_portfolio,config)
        
        self.portfolio = BasePortfolio(self)
        
        # Optional : recurring cashflow
        if self.config.recurring_investment:
            self.recurring_cashflow_dates = generate_recurring_dates(start_date,end_date, self.config.recurring_investment.frequency)