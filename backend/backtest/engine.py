import polars as pl
from datetime import date
from backend.backtest.portfolio import Portfolio
from backend.utils import round_shares, round_price, round_currency
from backend.models import TargetPortfolio, BacktestConfig
from backend.choices import OrderSide, RebalanceFrequency
from backend.pipelines.loader import get_backtest_data
from dateutil.relativedelta import relativedelta

class BacktestEngine:

    def __init__(self, start_date: date, end_date: date, target_portfolio: TargetPortfolio ,config: BacktestConfig):
            self.start_date = start_date
            self.end_date = end_date
            self.target_portfolio = target_portfolio
            self.config = config

            # Instantiate portfolio and data
            self.portfolio = Portfolio(self)
            self.backtest_data = self._load_backtest_data()
            self.master_calendar = self._generate_master_calendar()
            self.ticker_active_dates = self._generate_ticker_active_dates()
                       
            # Instantiate last rebalance day and order book
            self.last_rebalance_date = start_date
            self.pending_orders = None
            self.executed_orders = None

            # Optional : recurring cashflow
            if self.config.recurring_investment:
                self.recurring_cashflow_dates = self._generate_recurring_cashflow_dates()

            # Optional : dividends (if in manual mode)
            if self.config.mode == 'manual':
                self.dividend_dates = self._load_dividend_dates()

    def _load_backtest_data(self) -> pl.DataFrame:
        return get_backtest_data(
            self.config.mode, 
            self.target_portfolio.get_tickers(),
            self.start_date, 
            self.end_date
        )
    
    def _generate_master_calendar(self) -> pl.DataFrame:
        master_calendar = (
            self.backtest_data
            .group_by('date')
            .agg([
                pl.col('ticker').filter(pl.col('is_trading_day')==True).unique().sort().alias('trading_tickers')
            ])
            .sort('date')
        )
        return master_calendar
    
    def _generate_ticker_active_dates(self) -> pl.DataFrame:
        ticker_active_dates = (
            self.backtest_data
            .group_by('ticker')
            .agg([
                pl.col('date').min().alias('first_active_date'),
                pl.col('date').max().alias('last_active_date')
            ])
            .sort('ticker')
        )
        return ticker_active_dates
    
    def _generate_recurring_cashflow_dates(self) -> set[date]:
        dates = []
        cashflow_date = self.start_date
        while True:
            match self.config.recurring_investment.frequency:
                case 'daily':
                    cashflow_date += relativedelta(days=1)
                case 'weekly':
                    cashflow_date += relativedelta(weeks=1)
                case 'monthly':
                    cashflow_date += relativedelta(months=1)
                case 'quarterly':
                    cashflow_date += relativedelta(months=3)
                case 'yearly':
                    cashflow_date += relativedelta(years=1)
                case _:
                    raise ValueError('Invalid recurring investment frequency')
            if cashflow_date > self.end_date:
                break
            dates.append(cashflow_date)
        
        return set(dates)
    
    def _load_dividend_dates(self) -> set[date]:
        dividend_dates = (
            self.backtest_data
            .filter(pl.col('dividend').is_not_null())
            .get_column('date')
            .to_list()
        )
        return set(dividend_dates)

    def _find_active_tickers(self, date) -> set[str]:
        active_tickers = (
            self.ticker_active_dates
            .filter((pl.col('first_active_date')<=date)&(pl.col('last_active_date')>=date))
            .get_column('ticker')
            )
        
        if active_tickers.is_empty():
            raise ValueError(f'No active tickers on date : {date}')
        
        return set(active_tickers)
            
    def _find_trading_tickers(self, current_date: date) -> set[str]:
        trading_tickers = (
            self.master_calendar
            .filter(pl.col('date')==current_date)
            .get_column('trading_tickers')
            .explode()
        )
        return set(trading_tickers)
    
    def _all_active_tickers_trading(self, current_date: date) -> bool:
        active_tickers = self._find_active_tickers(current_date)
        trading_tickers = self._find_trading_tickers(current_date)

        return active_tickers==trading_tickers

    def _should_rebalance(self, current_date: date, last_rebalance_date: date, rebalance_frequency: RebalanceFrequency) -> bool:
        # rebalancing can not occur on days where live assets cannot be traded
        if not self._all_active_tickers_trading(current_date):
            return False
        else:
            match rebalance_frequency:
                case 'daily':
                    return True
                case 'weekly':
                    return current_date >= last_rebalance_date + relativedelta(weeks=1)
                case 'monthly':
                    return current_date >= last_rebalance_date + relativedelta(months=1)
                case 'quarterly':
                    return current_date >= last_rebalance_date + relativedelta(months=3)
                case 'yearly':
                    return current_date >= last_rebalance_date  + relativedelta(years=1)
                case _:
                    return False
                
    def _normalize_portfolio_targets(self, date: date) -> dict[str,float]:
        active_tickers = self._find_active_tickers(date)
        filtered_weights = {ticker: weight for ticker, weight in self.target_portfolio.weights.items() if ticker in active_tickers}

        total_weight = sum(filtered_weights.values())

        normalized_weights = {ticker : weight / total_weight for ticker, weight in filtered_weights.items()}
        
        return normalized_weights

    def _next_trading_date(self, ticker: str, target_date: date) -> date | None:
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
    
    def _get_ticker_allocations_by_target(self, normalized_weights: dict[str, float], total_value_to_allocate: float) -> dict[str, float]:
        return {ticker: weight*total_value_to_allocate for ticker, weight in normalized_weights.items()}
     
    def _queue_orders(self, current_date: date, ticker_allocations: dict[str, float], side : OrderSide = 'buy'):
        
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

    def _get_prices_on_date(self, current_date: date) -> dict[str,float]:
        prices_df = (
            self.backtest_data
            .filter(pl.col('date')==current_date)
            .select(['ticker','price'])
            .sort('ticker')         
        )

        return dict(zip(prices_df['ticker'], prices_df['price']))

    def _execute_orders(self, current_date: date, prices: dict[str, float]):

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

    def _get_dividends_on_date(self, current_date: date) -> dict[str,float]:

        dividends_df = (
            self.backtest_data
            .filter(pl.col('date')==current_date)
            .select(['ticker','dividend'])
            .sort('ticker')  
        )

        return dict(zip(dividends_df['ticker'], dividends_df['dividend']))
    

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
        if buy_order_targets:
            self._queue_orders(current_date,buy_order_targets,'buy')

        # Queue buy order for each under_allocated ticker
        if sell_order_targets:
            self._queue_orders(current_date,sell_order_targets, 'sell')

        # Update rebalance flag and last rebalance date
        self.portfolio.did_rebalance = True
        self.last_rebalance_date = current_date


    def run(self) -> tuple[dict[str, pl.DataFrame],pl.DataFrame]:
        """
        Executes the full backtest simulation over the configured date range.

        Returns:
            tuple[dict[str, pl.DataFrame], pl.DataFrame]: 
                - A dictionary containing daily snapshots:
                    - 'cash': Cash balances per day
                    - 'holdings': Holdings per asset per day
                    - 'dividends': Dividend records per day
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
            if self.config.mode == 'manual' and current_date in self.dividend_dates:
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

        # Bulk convert snapshots into polars dataframe for better processing
        history = {
            "cash":pl.DataFrame(cash_snapshots),
            "holdings":pl.DataFrame(holding_snapshots),
            "dividends":pl.DataFrame(dividend_snapshots)
        }

        # Combine order books 
        orders = pl.concat([self.executed_orders,self.pending_orders])

        return history, orders
    