import polars as pl
from datetime import date
from typing import Dict, Tuple, List, Optional
from backend.backtest.portfolio import Portfolio
from backend.utils import get_scheduling_dates
from backend.models import RecurringInvestment


class BacktestEngine:
    def __init__(self, portfolio: Portfolio,start_date: date, end_date: date, target_weights: Dict[str, float],price_data : Tuple[pl.DataFrame, List[date]],corporate_action_data: Tuple[pl.DataFrame, pl.DataFrame],recurring_investment: Optional[RecurringInvestment] = None):
            self.portfolio = portfolio
            self.start_date = start_date
            self.end_date = end_date
            self.target_weights = target_weights
            self.price_data = price_data
            self.corporate_action_data = corporate_action_data
            self.recurring_investment = recurring_investment

    def run(self) -> List[Dict[str, object]]:
        # unpack price data tuple
        all_prices, trading_dates = self.price_data
        
        # Create empty list for portfolio snapshots
        snapshots = []

        # Find rebalance dates
        rebalance_freq = self.portfolio.strategy.rebalance_frequency
        rebalance_dates = get_scheduling_dates(self.start_date,self.end_date,rebalance_freq,trading_dates)

        # Find recurring_investment dates if applicable
        if self.recurring_investment:
            investment_freq = self.recurring_investment.frequency
            investment_dates = get_scheduling_dates(self.start_date,self.end_date,investment_freq,trading_dates)

        # Find corporate action dates
        dividends, stock_splits = self.corporate_action_data
        dividend_dates = dividends['date'].to_list()
        stock_split_dates = stock_splits['date'].to_list()

        # Iterate through date range, rebalance and invest where necessary and save snapshot
        for row in all_prices.iter_rows(named=True):
            date = row['date']
            date_prices = {k: v for k, v in row.items() if k != 'date'}

            # Initialise snapshot flags
            cash_inflow = 0.0
            dividend_income = 0.0
            rebalanced = False
            invested = False
            dividends_received = False
            # stock_splits

            # Make initial investment
            if date == self.start_date:
                cash_inflow = self.portfolio.cash_balance
                self.portfolio.invest_by_target(self.target_weights,date_prices)
                invested = True
        
            # Rebalance
            if date in rebalance_dates:
                self.portfolio.rebalance(self.target_weights,date_prices)
                rebalanced = True

            # Dividends
            if date in dividend_dates:
                # Add dividend which were distributed on current date to portfolio
                dividend_dict = dividends.filter(pl.col('date')==date).drop('date').to_dicts()[0]
                self.portfolio.add_dividends(dividend_dict, self.portfolio.holdings)
                dividends_received = True
                # Process dividend
                if self.portfolio.strategy.reinvest_dividends:
                    cash_inflow += self.portfolio.get_total_dividends()
                    self.portfolio.invest_by_target(self.target_weights,date_prices)
                    invested = True
                else:
                    dividend_income += self.portfolio.get_total_dividends()
                    
            else:
                self.portfolio.dividends = {}

            # Recurring investment
            if self.recurring_investment:
                 if date in investment_dates:
                    self.portfolio.add_cash(self.recurring_investment.amount)
                    cash_inflow += self.recurring_investment.amount
                    self.portfolio.invest_by_target(self.target_weights,date_prices)
                    invested = True

            snapshots.append(self.portfolio.snapshot(date,date_prices,cash_inflow,dividend_income,rebalanced,invested,dividends_received))

        # Save snapshots to object
        self.history = snapshots

        return snapshots
    