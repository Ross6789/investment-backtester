from datetime import date
import polars as pl
from backend.core.models import BacktestConfig, BacktestResult
from backend.utils.scheduling import generate_recurring_dates
from backend.backtest.engine import BaseEngine
from backend.backtest.portfolios import BasicPortfolio

class BasicEngine(BaseEngine):

    def __init__(self,config: BacktestConfig, backtest_data: pl.DataFrame):
        """
        Initialize BasicBacktest with data and configuration

        Args:
            config (BacktestConfig): Strategy and backtest settings.
            backtest_data (pl.DataFrame): Market data.
        """
        # Run superclass constructor
        super().__init__(config, backtest_data)
       
        # Initialise specific portfolio for this mode
        self.portfolio = BasicPortfolio(self)

        # Scheduled rebalances : compute dates in advance
        rebalance_freq = self.config.strategy.rebalance_frequency.value
        self.rebalance_dates = (
            generate_recurring_dates(self.config.start_date,self.config.end_date, rebalance_freq)
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
        # Sell all assets : this is a simplified way of converting all holding value into cash instead of calling sell method for every ticker
        self.portfolio.cash_balance = self.portfolio.get_total_value(prices)
        self.portfolio.holdings = {}
        
        # Find balanced allocations
        target_allocations = self._get_ticker_allocations_by_target(normalized_target_weights,self.portfolio.cash_balance)

        # Re-buy holdings in target amounts
        for ticker, funds in target_allocations.items():
            self.portfolio.invest(ticker,funds,prices.get(ticker),True)

        # Update portfolio flag
        self.portfolio.did_rebalance = True


    def run(self) -> BacktestResult:
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
                - "data": Full backtest data used during run
                - "calendar": Master calendar with active and trading tickers per date
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

            if current_date == self.config.start_date:
                self.portfolio.add_cash(self.config.initial_investment)
                invested = False
            if current_date in self.cashflow_dates:
                self.portfolio.add_cash(self.config.recurring_investment.amount)
                invested = False

            # --- CHECK PORTFOLIO ACTIVE ---

            # Skip to next date if no tickers active active yet, but still need to take a cash snapshot 
            if current_date < self.first_active_date:
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
                    for ticker, amount in allocated_targets.items():
                        price = daily_prices.get(ticker)
                        self.portfolio.invest(ticker,amount,price,True)
                    invested = True
    
            # --- SNAPSHOTS ---

            # Fetch daily snapshot and add to relevant snapshot list
            daily_snapshot = self.portfolio.get_daily_snapshot(current_date,daily_prices)
            cash_snapshots.append(daily_snapshot['cash'])
            holding_snapshots.extend(daily_snapshot['holdings'])

        # Bulk convert snapshots into polars dataframe for better processing and package within backtest result dataclass
        result = BacktestResult(
            self.backtest_data,
            self.calendar_df,
            pl.DataFrame(cash_snapshots),
            pl.DataFrame(holding_snapshots)
            )

        return result
