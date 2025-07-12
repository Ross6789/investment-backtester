from abc import ABC, abstractmethod
from datetime import date
import polars as pl
from backend.models import RealisticBacktestResult, BacktestResult
from backend.utils import generate_recurring_dates
from backend.backtest.analysers import BaseAnalyser


class RealisticAnalyser(BaseAnalyser):
    def __init__(self, backtest_results : RealisticBacktestResult):
        super().__init__(backtest_results)
        self.dividends_lf = backtest_results.dividends.lazy()
        self.orders_lf = backtest_results.orders.lazy()

        # Enrich lazyframes
        self.cash_lf = self._add_buy_sell_bool_flags()

        print(backtest_results.data)
        print(backtest_results.calendar)
        print(backtest_results.cash)
        print(backtest_results.holdings)
        print(backtest_results.dividends)
        print(backtest_results.orders)

        print(self.cash_lf.collect())


    # --- Enriching result dataframes --- # 

    def _add_buy_sell_bool_flags(self) -> pl.LazyFrame:
        
        order_flags = (
            self.orders_lf
            .filter(pl.col('status') == 'fulfilled')
            .select([
                pl.col('date_executed').alias('date'),
                (pl.col('side') == 'buy').alias('did_buy'),
                (pl.col('side') == 'sell').alias('did_sell'),
            ])
        )

        cash_with_flags = (
            self.cash_lf.join(order_flags,on='date',how='left')
            .fill_null(False)
        )
        return cash_with_flags
    

    def _add_cumulative_dividend(self) -> pl.LazyFrame:
        
        order_flags = (
            self.orders_lf
            .filter(pl.col('status') == 'fulfilled')
            .select([
                pl.col('date_executed').alias('date'),
                (pl.col('side') == 'buy').alias('did_buy'),
                (pl.col('side') == 'sell').alias('did_sell'),
            ])
        )

        cash_with_flags = (
            self.cash_lf.join(order_flags,on='date',how='left')
            .fill_null(False)
        )
        return cash_with_flags
    
        
    # --- Generating formatted summary dataframes --- # 

    def generate_dividend_summary(self) -> pl.DataFrame:
        pass

    

    def generate_order_summary(self) -> pl.DataFrame:
        pass









    # def generate_dividend_summary(self) -> pl.LazyFrame:
        
    #     dividend_summary = (
    #         self.dividends_lf.join(self.holdings_lf, on=['date','ticker'],how='left')
    #         .select(['date','ticker','units','dividend_per_unit','total_dividend'])
    #     )
    #     return dividend_summary

import backend.config as config

save_path = config.get_parquet_backtest_result_path()

data = pl.read_parquet(save_path / 'backtest_data.parquet')
calendar = pl.read_parquet(save_path / 'backtest_calendar.parquet')
cash = pl.read_parquet(save_path / 'backtest_cash.parquet')
holdings = pl.read_parquet(save_path / 'backtest_holdings.parquet')
dividends = pl.read_parquet(save_path / 'backtest_dividends.parquet')
orders = pl.read_parquet(save_path / 'backtest_orders.parquet')

test_results = RealisticBacktestResult(data,calendar,cash,holdings,dividends,orders)
test_analyser = RealisticAnalyser(test_results)


