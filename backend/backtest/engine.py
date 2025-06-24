import polars as pl
from datetime import date
from typing import Dict, Tuple, List
from backend.backtest.portfolio import Portfolio

class BacktestEngine:
    def __init__(self, portfolio: Portfolio,start_date: date, end_date: date, target_weights: Dict[str, float], price_data : Tuple[pl.DataFrame, List[date]]):
            self.portfolio = portfolio
            self.start_date = start_date
            self.end_date = end_date
            self.target_weights = target_weights
            self.price_data = price_data

    def run(self) -> List[Dict[str, object]]:
        # unpack price data tuple
        all_prices, trading_dates = self.price_data
        
        # Create empty list for portfolio snapshots
        snapshots = []

        # Find rebalance dates
        rebalance_dates = self.portfolio.strategy.get_rebalance_dates(self.start_date,self.end_date,trading_dates)

        # Iterate through date range, rebalance where necessary and save snapshot
        for row in all_prices.iter_rows(named=True):
            date = row['date']
            date_prices = {k: v for k, v in row.items() if k != 'date'}
        
            if date in rebalance_dates:
                self.portfolio.rebalance(self.target_weights,date_prices)
            
            snapshots.append(self.portfolio.snapshot(date,date_prices))
        
        # Save snapshots to object
        self.history = snapshots

        return snapshots
    