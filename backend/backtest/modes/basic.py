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
        rebalance_freq = self.config.strategy.rebalance_frequency.value
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
        value = self.portfolio.get_total_value(prices)

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
        Runs the BASIC mode backtest over the full date range.

        Assumptions:
        - Immediate investment of all available cash (fractional shares allowed)
        - Uses adjusted prices
        - Cash accumulates if no tickers are active

        For each date:
        - Apply cash inflows
        - Skip dates before first tradable ticker
        - Fetch prices and reset portfolio metrics
        - Rebalance or invest cash based on target weights
        - Record daily snapshots

        Returns:
            dict[str, pl.DataFrame]: 
                - "cash": Daily cash balances
                - "holdings": Daily asset holdings
        """
        # Create empty list for portfolio snapshots
        cash_snapshots = []
        holding_snapshots = []

        # Iterate through date range in master calendar
        for current_date in self.calendar_df['date']:
    
            # Reset portfolio for the day
            self.portfolio.daily_reset()

            # --- HANDLE CASHFLOWS ---

            if current_date == self.start_date:
                self.portfolio.add_cash(self.config.initial_investment)
                invested = False
            if current_date in self.cashflow_dates:
                self.portfolio.add_cash(self.config.recurring_investment.amount)
                invested = False

            # --- CHECK PORTFOLIO ACTIVE ---

            # Skip to next date if no tickers active active/trading yet, but still need to take a cash snapshot 
            if current_date < self.first_trading_date:
                cash_snapshots.append(self.portfolio.get_cash_snapshot(current_date))
                continue

            # --- PRICE FETCH AND PORTFOLIO RESET ---

            # Fetch daily prices
            daily_prices = self._get_prices_on_date(current_date)

            # Reset target weights
            normalized_weights = None

            # --- MANAGE INVESTING ---
            
            # Calculate normalized targets if required
            if  not invested or current_date in self.rebalance_dates:
                normalized_weights = self._normalize_portfolio_targets(current_date)

                # Rebalancing overrides any other investing 
                if current_date in self.rebalance_dates:
                    self.rebalance(current_date,daily_prices,normalized_weights)

                # No rebalancing - safe to invest available cash based on target allocations
                else:
                    total_amount = self.portfolio.get_available_cash()
                    allocated_targets = self._get_ticker_allocations_by_target(normalized_weights,total_amount)
                    for ticker, amount in allocated_targets:
                        price = daily_prices.get(ticker)
                        self.portfolio.invest(ticker,amount,price,True)
                    invested = True
    
            # --- SNAPSHOTS ---

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