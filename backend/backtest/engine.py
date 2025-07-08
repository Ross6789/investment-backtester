import polars as pl
from datetime import date
from backend.backtest.portfolios.base_portfolio import BasePortfolio
from backend.models import TargetPortfolio, BacktestConfig
from backend.enums import OrderSide, RebalanceFrequency, BacktestMode, ReinvestmentFrequency
from dateutil.relativedelta import relativedelta

class BacktestEngine:
    """
    Runs portfolio backtests over a specified date range and dataset.

    Attributes:
        start_date (date): Start date of the backtest.
        end_date (date): End date of the backtest.
        backtest_data (pl.DataFrame): Full dataset of all tickers and dates.
        target_portfolio (TargetPortfolio): Desired portfolio allocation.
        config (BacktestConfig): Strategy and investment settings.
    """

    # --- Initialization & Setup ---

    def __init__(self, start_date: date, end_date: date, backtest_data: pl.DataFrame, target_portfolio: TargetPortfolio ,config: BacktestConfig):
            self.start_date = start_date
            self.end_date = end_date
            self.backtest_data = backtest_data
            self.target_portfolio = target_portfolio
            self.config = config



   
               

    # --- Portfolio Normalization ---

    def _normalize_portfolio_targets(self, date: date) -> dict[str,float]:
        """
        Normalize target portfolio weights for tickers active on the given date.

        Filters the target portfolio weights to include only tickers active on the specified date,
        then normalizes these weights so their sum equals 1.

        Args:
            date (date): The date to determine which tickers are active.

        Returns:
            dict[str, float]: A dictionary mapping active tickers to their normalized target weights.
        """
        active_tickers = self._find_active_tickers(date)
        filtered_weights = {ticker: weight for ticker, weight in self.target_portfolio.weights.items() if ticker in active_tickers}

        total_weight = sum(filtered_weights.values())

        normalized_weights = {ticker : weight / total_weight for ticker, weight in filtered_weights.items()}
        
        return normalized_weights

    
    def _get_ticker_allocations_by_target(self, normalized_weights: dict[str, float], total_value_to_allocate: float) -> dict[str, float]:
        """
        Calculate the dollar allocation for each ticker based on target normalized weights and total allocation amount.

        Args:
            normalized_weights (dict[str, float]): A dictionary mapping ticker symbols to their target portfolio weights (normalized to sum to 1).
            total_value_to_allocate (float): The total dollar amount available to allocate among the tickers.

        Returns:
            dict[str, float]: A dictionary mapping each ticker to the dollar amount allocated based on its target weight.
        """
        return {ticker: weight*total_value_to_allocate for ticker, weight in normalized_weights.items()}


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
            self.master_calendar
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

        for ticker, value in ticker_allocations.items():
            orders.append({
                    "ticker": ticker,
                    "value": value,
                    "date_placed": current_date,
                    "date_executed": self._next_trading_date(ticker,current_date),
                    "side": side,
                    'status': "pending"
                })
            
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
            - Moves executed orders from pending_orders to executed_orders.
        """
        executable_orders = (
            self.pending_orders
            .filter(pl.col('date_executed')==current_date)
        )

        updated_orders = []

        for row in executable_orders.iter_rows(named=True):
            ticker = row['ticker']
            value = row['value']
            side = row['side']
            price = prices.get(ticker)

            if price is None:
                raise ValueError(f'Order cannont be completed - missing price for ticker : {ticker} on date : {current_date}')
            
            match side:
                case 'buy':
                    fulfilled = self.portfolio.invest(ticker, value, price, self.config.strategy.allow_fractional_shares)
                case 'sell':
                    fulfilled = self.portfolio.sell(ticker, value, price, self.config.strategy.allow_fractional_shares)
                case _:
                    raise ValueError(f"Invalid order placed: side must be either 'buy' or 'sell', not {side}")

            row['status'] = "fulfilled" if fulfilled else "failed"
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


    # --- Price and Dividend lookup ---

    def _get_prices_on_date(self, current_date: date) -> dict[str,float]:
        """
        Retrieve prices for all tickers on the given date.

        Args:
            current_date (date): The date to fetch prices for.

        Returns:
            dict[str, float]: Mapping of ticker symbols to their prices on the date.
        """
        prices_df = (
            self.backtest_data
            .filter(pl.col('date')==current_date)
            .select(['ticker','price'])
            .sort('ticker')         
        )
        return dict(zip(prices_df['ticker'], prices_df['price']))
    

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
            .sort('ticker')  
        )
        return dict(zip(dividends_df['ticker'], dividends_df['dividend']))
    

    # --- Rebalancing ---

    def _should_rebalance(self, current_date: date, last_rebalance_date: date, rebalance_frequency: RebalanceFrequency) -> bool:
        """
        Determine whether the portfolio should be rebalanced on the current date.

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
                
                
    def rebalance(self, current_date: date, prices: dict[str, float], target_weights: dict[str, float]):
        # Find portfolio value
        total_value = self.portfolio.get_value(prices)

        buy_order_targets = {}
        sell_order_targets = {}

        # Determine target allocations
        for ticker, weight in target_weights.items():
            

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
        self.last_rebalance_date = current_date


    def run(self) -> dict[str, pl.DataFrame]:
        """
        Executes the full backtest simulation over the configured date range.

        Returns:
            tuple[dict[str, pl.DataFrame]: 
                - A dictionary containing daily snapshots:
                    - 'cash': Cash balances per day
                    - 'holdings': Holdings per asset per day
                    - 'dividends': Dividend records per day
                    - 'orders': all orders completed throuhgout the backtest
                - A combined Polars DataFrame of all orders (both fulfilled and failed), 
                including order status and execution metadata.
        """
        
        # Create empty list for portfolio snapshots
        cash_snapshots = []
        holding_snapshots = []
        dividend_snapshots = []

        # Iterate through date range in master calendar
        for current_date in self.master_calendar['date']:

            # Reset portfolio flags and daily metrics and order flag
            self.portfolio.daily_reset()
            place_order = False
            
            # Fetch daily prices
            daily_prices = self._get_prices_on_date(current_date)

            # Normalized target weights ie. the target portfolio based on active tickers, shoudl only be computed once and could eb reused iin both order and rebalance
            normalized_weights = None

            # MANAGE CASHFLOW 
            # Initial investment
            if current_date == self.master_calendar[0, 'date']:
                self.portfolio.add_cash(self.config.initial_investment)
                place_order = True

            # Recurring investment
            if current_date in self.recurring_cashflow_dates:
                self.portfolio.add_cash(self.config.recurring_investment.amount)
                place_order = True
            
            # Dividends
            if self.config.mode == BacktestMode.REALISTIC and current_date in self.dividend_dates:
                unit_dividend_per_ticker = self._get_dividends_on_date(current_date)
                dividends_earned = self.portfolio.process_dividends(unit_dividend_per_ticker)
                if self.config.strategy.reinvest_dividends:
                    self.portfolio.add_cash(dividends_earned)
                    place_order = True
                else:
                    self.portfolio.dividend_income = dividends_earned

            # PLACE INFLOW ORDERS
            if place_order:

                # Compute normalized weights if not done already for date
                if normalized_weights is None:
                    normalized_weights = self._normalize_portfolio_targets(current_date)

                # Inflow orders invest all available cash using target allocations (if wanted to just invest cashflow in for the day - add a cashflow var which sums daily cashflow)
                available_funds = self.portfolio.get_available_cash()
                ticker_allocations = self._get_ticker_allocations_by_target(normalized_weights,available_funds)

                self._queue_orders(current_date, ticker_allocations,'buy')

            # REBALANCE
            if self._should_rebalance(current_date,self.last_rebalance_date,self.config.strategy.rebalance_frequency):

                # Drop previous inflow orders which are to executed today, since the rebalancing may adjust quantities
                if self.pending_orders is not None:
                    self.pending_orders = self.pending_orders.filter(pl.col('date_executed') != current_date)
                
                # Compute normalized weights if not done already for date
                if normalized_weights is None:
                    normalized_weights = self._normalize_portfolio_targets(current_date)

                self.rebalance(current_date,daily_prices,normalized_weights)

            # EXECUTE ORDERS
            if self.pending_orders is not None:
                if not self.pending_orders.filter(pl.col('date_executed') == current_date).is_empty():
                    self._execute_orders(current_date,daily_prices)

            # Fetch daily snapshot and add to relevant snapshot list
            daily_snapshot = self.portfolio.get_daily_snapshot(current_date,daily_prices)
            cash_snapshots.append(daily_snapshot['cash'])
            holding_snapshots.extend(daily_snapshot['holdings'])
            dividend_snapshots.extend(daily_snapshot['dividends'])

        # Combine order books 
        orders = pl.concat([self.executed_orders,self.pending_orders])

        # Bulk convert snapshots into polars dataframe for better processing and package within dictionary
        history = {
            "cash":pl.DataFrame(cash_snapshots),
            "holdings":pl.DataFrame(holding_snapshots),
            "dividends":pl.DataFrame(dividend_snapshots),
            "orders": orders
        }

        return history
    