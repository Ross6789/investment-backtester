import polars as pl
from datetime import date
from typing import Dict, Tuple, List, Optional
from backend.backtest.portfolio import Portfolio, Strategy
from backend.utils import get_scheduling_dates
from backend.models import RecurringInvestment


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

    def __init__(self, start_date: date, end_date: date, target_weights: Dict[str, float],backtest_data : pl.DataFrame,mode: str = 'manual',initial_balance: float = 1000,recurring_investment: Optional[RecurringInvestment] = None, fractional_shares: bool = False, reinvest_dividends: bool = True, rebalance_frequency: str = 'never'):
            self.start_date = start_date
            self.end_date = end_date
            self.target_weights = target_weights
            self.backtest_data = backtest_data
            self.mode = mode
            self.recurring_investment = recurring_investment
            self.portfolio = Portfolio(initial_balance, Strategy(fractional_shares,reinvest_dividends,rebalance_frequency),self)
            self.master_calendar = self._generate_master_calendar()

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


    def _generate_rebalance_dates(self) -> List[pl.Date]:
        pass

    def _generate_invest_dates(self) -> List[pl.Date]:
        pass
        
    
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

        # Find unique dates in backtest 
        dates = self.backtest_data.select('date').unique().to_series().sort()

        # Iterate through date range, rebalance and collect daily data
        for current_date in dates:
            
            # Initialise snapshot flags
            cash_inflow = 0.0
            dividend_income = 0.0
            rebalanced = False
            invested = False
            dividends_received = False

            # Create daily dict for each ticker
            daily_dict = self._build_daily_dict(current_date)

            # Make initial investment
            if current_date == _next_trading_date(start_date):
                cash_inflow = self.portfolio.cash_balance
                self.portfolio.invest_by_target(self.target_weights,date_prices)
                invested = True
        
            # Rebalance
            if current_date in rebalance_dates:
                self.portfolio.rebalance(self.target_weights,date_prices)
                rebalanced = True

            # Dividends
            if (self.mode == 'manual') and (current_date in dividend_dates):
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

            # Recurring investment
            if self.recurring_investment:
                 if current_date in investment_dates:
                    self.portfolio.add_cash(self.recurring_investment.amount)
                    cash_inflow += self.recurring_investment.amount
                    self.portfolio.invest_by_target(self.target_weights,date_prices)
                    invested = True

            snapshots.append(self.portfolio.snapshot(current_date,date_prices,cash_inflow,dividend_income,rebalanced,invested,dividends_received))

        # Save snapshots to object
        self.history = snapshots

        return snapshots