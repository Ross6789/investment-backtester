from backend.backtest.modes.base import BaseBacktest
from backend.backtest.portfolios.basic import BasicPortfolio
from datetime import date
import polars as pl
from backend.models import TargetPortfolio, BacktestConfig, RebalanceFrequency
from backend.utils import generate_recurring_dates

class BasicBacktest(BaseBacktest):

    def __init__(self, start_date: date, end_date: date, backtest_data: pl.DataFrame, target_portfolio: TargetPortfolio ,config: BacktestConfig):
        """
        Initialize BasicBacktest with data, portfolio, and rebalance schedule.

        Args:
            start_date (date): Backtest start date.
            end_date (date): Backtest end date.
            backtest_data (pl.DataFrame): Market data.
            target_portfolio (TargetPortfolio): Target portfolio config.
            config (BacktestConfig): Strategy and backtest settings.
        """
        # Run superclass constructor
        super().__init__(start_date, end_date, backtest_data, target_portfolio,config)
       
        # Initialise specific portfolio for this mode
        self.portfolio = BasicPortfolio(self)

        # Scheduled rebalances : compute dates in advance
        rebalance_freq = self.config.strategy.rebalance_frequency
        self.rebalance_dates = (
            generate_recurring_dates(start_date,end_date, rebalance_freq)
            if rebalance_freq != 'never' else set()
        )
    

    def rebalance(self, current_date: date, prices: dict[str, float], normalized_target_weights: dict[str, float]) -> None:
        """
        Rebalance the portfolio to align with the target asset weightings.

        Args:
            current_date (date): The current simulation date used to determine active tickers.
            prices (dict[str, float]): A mapping of ticker symbols to their current prices.
            normalized_target_weights (dict[str, float]): Target asset weightings normalized to active tickers.
        """
        # Find total portfolio value
        value = self.portfolio.get_total_holdings_value(prices)

        # Find balanced allocations
        target_allocations = self._get_ticker_allocations_by_target(normalized_target_weights,value)

        # Reset holdings and re-buy in target amounts
        self.portfolio.holdings = {}
        for ticker, funds in target_allocations.items():
            self.portfolio.invest(ticker,funds,prices.get(ticker),True)

        # Update portfolio flag
        self.portfolio.did_rebalance = True


    def run(self) -> dict[str, pl.DataFrame]:
        """
        Executes the backtest over the defined date range.

        Iterates through each date in the master calendar to:
        - Reset daily portfolio metrics
        - Process initial and recurring cash inflows
        - Invest cash inflows according to target allocations
        - Rebalance portfolio on scheduled dates
        - Capture daily portfolio snapshots of cash and holdings

        Returns:
            dict[str, pl.DataFrame]: Dictionary containing time series of daily cash and holdings snapshots.
        """
        # Create empty list for portfolio snapshots
        cash_snapshots = []
        holding_snapshots = []

        # Iterate through date range in master calendar
        for current_date in self.master_calendar_df['date']:

            # Reset portfolio flags and daily metrics and order flag
            self.portfolio.daily_reset()
            normalized_weights = None
            
            # Fetch daily prices
            daily_prices = self._get_prices_on_date(current_date)

            # Cash inflows
            if current_date == self.start_date:
                self.portfolio.add_cash(self.config.initial_investment)
            if current_date in self.cashflow_dates:
                self.portfolio.add_cash(self.config.recurring_investment.amount)
            daily_cashflow = self.portfolio.cash_inflow

            # Calculate normalized targets if required
            if  daily_cashflow > 0 or current_date in self.rebalance_dates:
                normalized_weights = self._normalize_portfolio_targets(current_date)

            # Invest daily cashflow
            if daily_cashflow > 0:
                allocated_targets = self._get_ticker_allocations_by_target(normalized_weights,daily_cashflow)
                self.portfolio.invest(daily_cashflow,allocated_targets,daily_prices,True)
    
            # Rebalance
            if current_date in self.rebalance_dates:
                self.rebalance(current_date,daily_prices,normalized_weights)

            # Fetch daily snapshot and add to relevant snapshot list
            daily_snapshot = self.portfolio.get_daily_snapshot(current_date,daily_prices)
            cash_snapshots.append(daily_snapshot['cash'])
            holding_snapshots.extend(daily_snapshot['holdings'])

        # Bulk convert snapshots into polars dataframe for better processing and package within dictionary
        history = {
            "cash":pl.DataFrame(cash_snapshots),
            "holdings":pl.DataFrame(holding_snapshots),
        }

        return history


    def analyse(self):
        pass