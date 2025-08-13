from datetime import date
import polars as pl
from dateutil.relativedelta import relativedelta
from backend.core.models import BacktestConfig, RealisticBacktestResult
from backend.core.enums import OrderSide, RebalanceFrequency
from backend.backtest.engines import BaseEngine
from backend.backtest.portfolios import RealisticPortfolio

class RealisticEngine(BaseEngine):

    def __init__(self, config: BacktestConfig, backtest_data: pl.DataFrame):
        """
        Initialize the realistic backtest mode.

        Sets up the portfolio, rebalance tracking, order books, and dividend schedule.
        This mode models more realistic behavior, including cashflow timing, order queuing, 
        and delayed execution.

        Args:
            config (BacktestConfig): Configuration object specifying backtest parameters and strategy.
            backtest_data (pl.DataFrame): Historical market data used in the backtest.
        """
        # Run superclass constructor
        super().__init__(config,backtest_data)

        # Initialise specific portfolio for this mode
        self.portfolio = RealisticPortfolio(self)

        # Instantiate previous rebalance day (set as first day a ticker is trading) and order books
        self.previous_rebalance_date = self._get_first_active_date()
        self.pending_orders = None
        self.executed_orders = None

        # Load dividend dates
        self.dividend_dates = self._load_dividend_dates()


    # --- Data Generation & Loading ---
    
    def _load_dividend_dates(self) -> set[date]:
        """
        Extract all unique dates from the backtest data where dividends were issued.

        Returns:
            set[date]: A set of dates on which at least one ticker paid a dividend.
        """
        dividend_dates = (
            self.backtest_data
            .filter(pl.col('dividend').is_not_null())
            .get_column('date')
            .to_list()
        )
        return set(dividend_dates)


    # --- Order Management ---

    def _next_trading_date(self, ticker: str, target_date: date) -> date | None:
        """
        Find the next trading date on or after the target date for the given ticker.

        Args:
            ticker (str): The ticker symbol to look for in the trading calendar.
            target_date (date): The date from which to search forward (inclusive).

        Returns:
            date | None: The next trading date on or after `target_date` when the ticker is tradable.
                        Returns None if no such date exists in the calendar.
        """
        trading_date = (
            self.calendar_df
            .filter((pl.col('date') >= target_date) & (pl.col('trading_tickers').list.contains(ticker)))
            .sort('date')
            .select('date')
            .limit(1)
        )
        if trading_date.is_empty():
            return None
        return trading_date[0, 0]

     
    def _queue_orders(self, current_date: date, ticker_allocations: dict[str, float], side : OrderSide = 'buy'):
        """
        Create and add new orders for each ticker with allocated value to the pending orders.

        Args:
            current_date (date): Date when orders are placed.
            ticker_allocations (dict[str, float]): Mapping of tickers to allocation amounts.
            side (OrderSide, optional): Order side ('buy' or 'sell'). Defaults to 'buy'.
        """
        orders = []

        for ticker, target_value in ticker_allocations.items():
            if target_value > 0.01: # only queue any orders more than 1 pence ie. guard against very small floating values
                orders.append({
                        "ticker": ticker,
                        "target_value": target_value,
                        "date_placed": current_date,
                        "date_executed": self._next_trading_date(ticker,current_date),
                        "side": side,
                        "base_price": None,
                        "units": None,
                        'status': "pending"
                    })
            
        if not orders:
            return # Exit method if no valid order to add to queue
            
        new_orders_df = pl.DataFrame(orders)
        
        if self.pending_orders is None:
            self.pending_orders = new_orders_df
        else:
            self.pending_orders = pl.concat([self.pending_orders, new_orders_df])

    def _execute_orders(self, current_date: date, prices: dict[str, float]):
        """
        Execute all pending orders scheduled for the current date using given prices.

        Args:
            current_date (date): The date on which to execute orders.
            prices (dict[str, float]): Mapping of tickers to their prices on current_date.

        Raises:
            ValueError: If price for a ticker is missing or order side is invalid.

        Updates:
            - Marks orders as 'fulfilled' or 'failed' based on portfolio transaction success.
            - Adds 'units' column to indicate number of units bought or sold.
            - Adds 'base_price' column to record the price used for execution.
            - Moves executed orders from pending_orders to executed_orders.
        """
        executable_orders = (
            self.pending_orders
            .filter(pl.col('date_executed')==current_date)
        )

        updated_orders = []

        for row in executable_orders.iter_rows(named=True):
            ticker = row['ticker']
            target_value = row['target_value']
            side = row['side']
            price = prices.get(ticker)

            if price is None:
                raise ValueError(f'Order cannont be completed - missing price for ticker : {ticker} on date : {current_date}')
            
            match side:
                case 'buy':
                    units_moved = self.portfolio.invest(ticker, target_value, price, self.config.strategy.allow_fractional_shares)
                case 'sell':
                    units_moved = self.portfolio.sell(ticker, target_value, price, self.config.strategy.allow_fractional_shares)
                case _:
                    raise ValueError(f"Invalid order placed: side must be either 'buy' or 'sell', not {side}")

            row['base_price'] = price
            row['units'] = units_moved
            row['status'] = "fulfilled" if units_moved > 0 else "failed"
            
            updated_orders.append(row)

        # Create new dataframe with updated orders
        orders_executed_today = pl.DataFrame(updated_orders)

        # Append executed orders to executed_orders DataFrame
        if self.executed_orders is None:
            self.executed_orders = orders_executed_today
        else:
            self.executed_orders = pl.concat([self.executed_orders, orders_executed_today])

        # Remove executed orders from pending_orders
        self.pending_orders = self.pending_orders.filter(pl.col('date_executed') != current_date)


    def _get_dividends_on_date(self, current_date: date) -> dict[str,float]:
        """
        Retrieve dividend amounts for each ticker on a specific date.

        Args:
            current_date (date): The date to get dividends for.

        Returns:
            dict[str, float]: Mapping of ticker symbols to their dividend values on the date.
        """
        dividends_df = (
            self.backtest_data
            .filter(pl.col('date')==current_date)
            .select(['ticker','dividend'])
        )
        return dict(zip(dividends_df['ticker'], dividends_df['dividend']))


    # --- Ticker trading check ---

    def _all_active_tickers_trading(self, date: date) -> bool:
        """
        Check whether all active tickers on a given date are also trading.

        Args:
            date (date): The date to check.

        Returns:
            bool: True if all active tickers are trading and at least one ticker is active,
                False otherwise.
        """
        day_info = self.calendar_dict.get(date, {})
        active_tickers = day_info.get("active_tickers", set())
        trading_tickers = day_info.get("trading_tickers", set())

        return active_tickers == trading_tickers and len(active_tickers) > 0


    # --- Rebalancing ---

    def _should_rebalance(self, current_date: date, last_rebalance_date: date, rebalance_frequency: RebalanceFrequency) -> bool:
        """
        Dynamically determine whether the portfolio should be rebalanced on the current date.

        Rebalancing only occurs if all active tickers are trading on the current date
        and the specified rebalance frequency interval has elapsed since the last rebalance.

        Args:
            current_date (date): The date to check for rebalancing.
            last_rebalance_date (date): The date when the portfolio was last rebalanced.
            rebalance_frequency (RebalanceFrequency): Frequency at which rebalancing should occur.

        Returns:
            bool: True if rebalancing should occur on current_date, False otherwise.

        Raises:
            ValueError: If the provided rebalance_frequency is invalid.
        """
        if not self._all_active_tickers_trading(current_date):
            return False
        else:
            match rebalance_frequency:
                case RebalanceFrequency.DAILY:
                    return True
                case RebalanceFrequency.WEEKLY:
                    return current_date >= last_rebalance_date + relativedelta(weeks=1)
                case RebalanceFrequency.MONTHLY:
                    return current_date >= last_rebalance_date + relativedelta(months=1)
                case RebalanceFrequency.QUARTERLY:
                    return current_date >= last_rebalance_date + relativedelta(months=3)
                case RebalanceFrequency.YEARLY:
                    return current_date >= last_rebalance_date  + relativedelta(years=1)
                case _:
                    raise ValueError(f"Invalid rebalance frequency: {rebalance_frequency}")
                
                
    def rebalance(self, current_date: date, prices: dict[str, float], normalized_target_weights: dict[str, float]) -> None:
        """
        Adjust portfolio holdings to match target weights by generating buy and sell orders.

        Args:
            current_date (date): The date on which rebalancing is performed.
            prices (dict[str, float]): Current market prices for each ticker.
            normalized_target_weights (dict[str, float]): Target portfolio weights for each ticker, normalized to sum to 1.

        Behavior:
            - Calculates the target dollar allocation for each ticker based on total portfolio value.
            - Compares with current holding values to determine buy or sell amounts.
            - Queues sell orders for tickers over-allocated and buy orders for tickers under-allocated.
            - Updates rebalance status and records the date of last rebalance.
        """
        # Find portfolio value
        total_value = self.portfolio.get_total_value(prices)

        buy_order_targets = {}
        sell_order_targets = {}

        # Determine target allocations
        for ticker, weight in normalized_target_weights.items():
            

            target_value = total_value * weight
            actual_value = self.portfolio.holdings.get(ticker, 0.0) * prices.get(ticker, 0.0)
            
            correction_value = target_value - actual_value

            if correction_value > 0:
                buy_order_targets[ticker] = correction_value
            elif correction_value < 0:
                sell_order_targets[ticker] = -correction_value


        # Queue sell order for each over-allocated ticker
        if sell_order_targets:
            self._queue_orders(current_date,sell_order_targets, 'sell')

        # Queue buy order for each under_allocated ticker
        if buy_order_targets:
            self._queue_orders(current_date,buy_order_targets,'buy')

        # Update rebalance flag and last rebalance date
        self.portfolio.did_rebalance = True
        self.previous_rebalance_date = current_date


    def run(self) -> RealisticBacktestResult:
        """
        Executes the full portfolio backtest over the master calendar date range.

        This method simulates day-by-day portfolio activity, including:
        - Applying initial and recurring cash injections
        - Processing dividends (with or without reinvestment)
        - Determining whether to place new buy orders based on cash inflow
        - Evaluating and applying rebalancing strategies at defined intervals
        - Executing pending orders using available market prices
        - Recording daily snapshots of cash, holdings, and dividends

        Returns:
            dict[str, pl.DataFrame]: A dictionary containing historical portfolio data:
                - "data": Full backtest data used during run
                - "calendar": Master calendar with active and trading tickers per date
                - "cash": Daily cash balances
                - "holdings": Daily asset holdings
                - "dividends": Dividend income earned or reinvested
                - "orders": All executed and pending orders throughout the backtest
        """
        # Initialize empty lists for portfolio snapshots
        cash_snapshots = []
        holding_snapshots = []
        dividend_snapshots = []

        # Iterate through date range in master calendar
        for current_date in self.calendar_df['date']:

            # Reset portfolio for the day
            self.portfolio.daily_reset()
            place_order = False

            # --- HANDLE CASHFLOWS ---

            # Initial investment
            if current_date == self.config.start_date:
                self.portfolio.add_cash(self.config.initial_investment)
                place_order = True

            # Recurring investment
            if current_date in self.cashflow_dates:
                self.portfolio.add_cash(self.config.recurring_investment.amount)
                place_order = True

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

            # --- DIVIDENDS ---
            
            # Dividends
            if current_date in self.dividend_dates:
                unit_dividend_per_ticker = self._get_dividends_on_date(current_date)
                dividends_earned = self.portfolio.process_dividends(unit_dividend_per_ticker)
                if self.config.strategy.reinvest_dividends:
                    self.portfolio.add_cash(dividends_earned)
                    place_order = True
                else:
                    self.portfolio.dividend_income = dividends_earned

            # --- QUEUE ORDERS ---

            # Determine if rebalacing will occur
            rebalancing = self._should_rebalance(current_date,self.previous_rebalance_date,self.config.strategy.rebalance_frequency)

            # If there's cash to invest or a rebalance scheduled, compute the normalized target weights for each ticker
            if place_order or rebalancing:
                normalized_weights = self._normalize_portfolio_targets(current_date)

                # Rebalancing overrides any inflow buy orders 
                if rebalancing:
                    self.rebalance(current_date,daily_prices,normalized_weights)

                # No rebalancing - safe to invest available cash based on target allocations
                else:
                    available_funds = self.portfolio.get_available_cash()
                    ticker_allocations = self._get_ticker_allocations_by_target(normalized_weights,available_funds)
                    self._queue_orders(current_date, ticker_allocations,'buy')

            # --- EXECUTE ORDERS ---

            if self.pending_orders is not None: 
                if not self.pending_orders.filter(pl.col('date_executed') == current_date).is_empty():
                    self._execute_orders(current_date,daily_prices)

            # --- RECORD SNAPSHOTS ---

            daily_snapshot = self.portfolio.get_daily_snapshot(current_date,daily_prices)
            cash_snapshots.append(daily_snapshot['cash'])
            holding_snapshots.extend(daily_snapshot['holdings'])
            dividend_snapshots.extend(daily_snapshot['dividends'])

        # Combine order books 
        orders = pl.concat([self.executed_orders,self.pending_orders])

        # Bulk convert snapshots into polars dataframe for better processing and package within result dataclass
        result = RealisticBacktestResult(
            self.backtest_data,
            self.calendar_df,
            pl.DataFrame(cash_snapshots),
            pl.DataFrame(holding_snapshots),
            pl.DataFrame(dividend_snapshots),
            orders
        )

        return result
