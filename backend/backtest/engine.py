import polars as pl
from datetime import date
from typing import Dict, Set, List, Optional
from backend.backtest.portfolio import Portfolio, Strategy
from backend.utils import round_down
from backend.models import RecurringInvestment, TargetPortfolio
from backend.pipelines.loader import get_backtest_data
from backend.config import get_backtest_data_path
from dateutil.relativedelta import relativedelta



class BacktestEngine:
    """
    A backtesting engine to simulate a portfolio's performance over time 
    based on historical price and corporate action data.

    Args:
        start_date (date): The start date of the backtest.
        end_date (date): The end date of the backtest.
        target_weights (Dict[str, float]): Target allocation weights for each asset.
        price_data (Tuple[pl.DataFrame, List[date]]): Tuple of price DataFrame and list of trading dates.
        corporate_action_data (Tuple[pl.DataFrame, pl.DataFrame]): Tuple of dividend and stock split data.
        mode (str): Either 'manual' or 'auto'. Determines how corporate actions are handled.
        initial_balance (float): Starting cash balance for the portfolio.
        recurring_investment (Optional[RecurringInvestment]): Recurring investment configuration, if any.
        fractional_shares (bool): Whether to allow fractional share investing.
        reinvest_dividends (bool): Whether to reinvest dividends automatically.
        rebalance_frequency (str): Rebalancing frequency (e.g. 'monthly', 'quarterly', 'never').

    Attributes:
        history (List[Dict[str, object]]): A list of portfolio snapshots over time.
        portfolio (Portfolio): The Portfolio object tracking holdings and strategy.
    """

    def __init__(self, start_date: date, end_date: date, target_portfolio: TargetPortfolio ,mode: str = 'manual',initial_balance: float = 1000,recurring_investment: Optional[RecurringInvestment] = None, fractional_shares: bool = False, reinvest_dividends: bool = True, rebalance_frequency: str = 'never'):
            self.start_date = start_date
            self.end_date = end_date
            self.target_portfolio = target_portfolio
            self.mode = mode
            self.initial_balance = initial_balance
            self.recurring_investment = recurring_investment
            self.portfolio = Portfolio(Strategy(fractional_shares,reinvest_dividends,rebalance_frequency),self)
            self.backtest_data = self._load_backtest_data()
            self.master_calendar = self._generate_master_calendar()
            self.ticker_active_dates = self._generate_ticker_active_dates()
            self.recurring_cashflow_dates = self._generate_recurring_cashflow_dates()
            self.dividend_dates = self._load_dividend_dates()
            self.last_rebalance_date = start_date
            self.order_book = None

    def _load_backtest_data(self) -> pl.DataFrame:
        return get_backtest_data(
            get_backtest_data_path(), 
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
            .filter(pl.col('is_trading_day')==True)
            .group_by('ticker')
            .agg([
                pl.col('date').min().alias('first_active_date'),
                pl.col('date').max().alias('last_active_date')
            ])
            .sort('ticker')
        )
        return ticker_active_dates
    
    def _generate_recurring_cashflow_dates(self) -> Set[date]:
        dates = []
        cashflow_date = self.start_date
        while True:
            match self.recurring_investment.frequency.lower():
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
    
    def _load_dividend_dates(self) -> Set[date]:
        dividend_dates = (
            self.backtest_data
            .filter(pl.col('dividends').is_not_null())
            .select('date')
            .to_series()
            .to_list()
        )
        return set(dividend_dates)

    def _find_active_tickers(self, date) -> Set[str]:
        active_tickers = (
            self.ticker_active_dates
            .filter((pl.col('first_active_date')<=date)&(pl.col('last_active_date')>=date))
            .select('ticker')
            .to_series()
            )
        return set(active_tickers)
            
    def _find_trading_ticker(self, current_date: date) -> Set[str]:
        trading_tickers = (
            self.master_calendar
            .filter(pl.col('date')==current_date)
            .select('trading_tickers')
            .to_series()
            .explode()
        )
        return set(trading_tickers)
    
    def _all_active_tickers_trading(self, current_date: date) -> bool:
        active_tickers = self._find_active_tickers(current_date)
        trading_tickers = self._find_trading_ticker(current_date)

        return active_tickers==trading_tickers
    
    def _build_daily_dict(self, date: pl.Date) -> dict:
        daily_data = self.backtest_data.filter(pl.col('date')== date)
        
        return {
            row['ticker']: {
                'adj_close': row['adj_close'],
                'close': row['close'],
                'is_trading_day': row['is_trading_day'],
                'dividend': row['dividend'],
                'stock_split': row['stock_split']
            }
            for row in daily_data.iter_rows(named=True)
        }

    def _should_rebalance(self, current_date: date) -> bool:
        # rebalancing can not occur on days where live assets cannot be traded
        if not self._all_active_tickers_trading(current_date):
            return False
        else:
            freq = self.portfolio.strategy.rebalance_frequency
            last_rebalance = self.last_rebalance_date
            match freq :
                case 'daily':
                    return True
                case 'weekly':
                    return current_date >= last_rebalance + relativedelta(weeks=1)
                case 'monthly':
                    return current_date >= last_rebalance+ relativedelta(months=1)
                case 'quarterly':
                    return current_date >= last_rebalance + relativedelta(months=3)
                case 'yearly':
                    return current_date >= last_rebalance  + relativedelta(years=1)
                case _:
                    return False
                
    def _normalize_portfolio_targets(self, date: date) -> Dict[str,float]:
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
    
    def _queue_order(self, current_date: date):
        
        normalized_weights = self._normalize_portfolio_targets(current_date)
        orders = []
        available_cash = self.portfolio.cash

        for ticker, weight in normalized_weights.items():
            orders.append({
                    "ticker": ticker,
                    "allocated_funds": round_down(weight * available_cash, 2),
                    "date_placed": current_date,
                    "date_executed": self._next_trading_date(ticker,current_date)
                })
            
        new_orders_df = pl.DataFrame(orders)
        
        if self.order_book is None:
            self.order_book = new_orders_df
        else:
            self.order_book = pl.concat([self.order_book, new_orders_df])

    def _get_prices_on_date(self, current_date: date) -> Dict[str,float]:
        prices_df = (
            self.backtest_data
            .filter(pl.col('date')==current_date)
            .select(['ticker','price'])
            .sort('ticker')         
        )

        return dict(zip(prices_df['ticker'], prices_df['price']))

    def _process_orders(self, current_date: date):

        executable_orders = (
            self.order_book
            .filter(pl.col('date_executed')==current_date)
        )

        prices = self._get_prices_on_date(current_date)

        for ticker, allocated_funds in executable_orders.iter_rows():
            self.portfolio.invest(ticker, allocated_funds, prices)
            

    def run(self) -> List[Dict[str, object]]:
        """
        Runs the backtest simulation over the date range.

        Returns:
            List[Dict[str, object]]: A list of portfolio snapshots for each trading day,
            including portfolio state, cash inflow, dividend income, and flags for events
            like rebalancing and investing.
        """
        
        # # Create empty list for portfolio snapshots
        # snapshots = []

        # # Find rebalance dates
        # rebalance_freq = self.portfolio.strategy.rebalance_frequency
        # rebalance_dates = get_scheduling_dates(self.start_date,self.end_date,rebalance_freq,trading_dates)

        # # Find recurring_investment dates if applicable
        # investment_dates = []
        # if self.recurring_investment:
        #     investment_freq = self.recurring_investment.frequency
        #     investment_dates = get_scheduling_dates(self.start_date,self.end_date,investment_freq,trading_dates)

        # # Find dividend dates
        # dividend_dates = []
        # if self.mode == 'manual':
        #    dividend_dates = (
        #        self.backtest_data
        #        .filter(pl.col('date').is_not_null())
        #        .select('date')
        #        .to_series()
        #        .to_list()
        #        )

        # # Iterate through date range, rebalance and invest where necessary and save snapshot
        # for row in self.backtest_data.iter_rows(named=True):
        #     current_date = row['date']
        #     date_prices = {k: v for k, v in row.items() if k != 'date'}

        #     # Initialise snapshot flags
        #     cash_inflow = 0.0
        #     dividend_income = 0.0
        #     rebalanced = False
        #     invested = False
        #     dividends_received = False

        #     # Make initial investment
        #     if current_date == self.start_date:
        #         cash_inflow = self.portfolio.cash_balance
        #         self.portfolio.invest_by_target(self.target_weights,date_prices)
        #         invested = True
        
        #     # Rebalance
        #     if current_date in rebalance_dates:
        #         self.portfolio.rebalance(self.target_weights,date_prices)
        #         rebalanced = True

        #     # Dividends
        #     if (self.mode == 'manual') and (current_date in dividend_dates):
        #         # Add dividend which were distributed on current date to portfolio
        #         dividend_dict = dividends.filter(pl.col('date')==current_date).drop('date').to_dicts()[0]
        #         self.portfolio.add_dividends(dividend_dict, self.portfolio.holdings)
        #         dividends_received = True
        #         # Process dividend
        #         total_dividend = self.portfolio.get_total_dividends()
        #         if self.portfolio.strategy.reinvest_dividends:
        #             self.portfolio.add_cash(total_dividend)
        #             cash_inflow += total_dividend
        #             self.portfolio.invest_by_target(self.target_weights,date_prices)
        #             invested = True
        #         else:
        #             dividend_income += total_dividend
                    
        #     else:
        #         self.portfolio.dividends = {}

        #     # Recurring investment
        #     if self.recurring_investment:
        #          if current_date in investment_dates:
        #             self.portfolio.add_cash(self.recurring_investment.amount)
        #             cash_inflow += self.recurring_investment.amount
        #             self.portfolio.invest_by_target(self.target_weights,date_prices)
        #             invested = True

        #     snapshots.append(self.portfolio.snapshot(current_date,date_prices,cash_inflow,dividend_income,rebalanced,invested,dividends_received))

        # # Save snapshots to object
        # self.history = snapshots

        # return snapshots
    
        # Create empty list for portfolio snapshots
        snapshots = []

        # Iterate through date range in master calendar
        for current_date in self.master_calendar.iter_rows():
            
            # Initialise snapshot flags
            cash_inflow = 0.0
            dividend_income = 0.0
            rebalanced = False
            invested = False
            dividends_received = False

            # Create daily dict for each ticker
            daily_dict = self._build_daily_dict(current_date)

            # MANAGE CASHFLOW 
            # Initial investment
            if current_date == self.start_date:
                # queue order
                self._queue_order(current_date, self.initial_balance)
                self.portfolio.add_cash(self.initial_balance)

            # Recurring investment
            if current_date in self.recurring_cashflow_dates:
                # queue order

            # Dividends
            if (self.mode == 'manual') and (current_date in self.dividend_dates):
                # Add dividend which were distributed on current date to portfolio
                dividend_dict = dividends.filter(pl.col('date')==current_date).drop('date').to_dicts()[0]
                self.portfolio.add_dividends(dividend_dict, self.portfolio.holdings)
                dividends_received = True
                # Process dividend
                total_dividend = self.portfolio.get_total_dividends()
                if self.portfolio.strategy.reinvest_dividends:
                    self.portfolio.add_cash(total_dividend)
                    cash_inflow += total_dividend
                    self.portfolio.invest_by_target(self.target_weights,date_prices)
                    invested = True
                else:
                    dividend_income += total_dividend
                    
            else:
                self.portfolio.dividends = {}
            

            # PROCESS ORDERS
            
            
            # REBALANCE
            if self._should_rebalance(current_date):
                #Rebalance method
                pass

            snapshots.append(self.portfolio.snapshot(current_date,date_prices,cash_inflow,dividend_income,rebalanced,invested,dividends_received))

        # Save snapshots to object
        self.history = snapshots

        return snapshots