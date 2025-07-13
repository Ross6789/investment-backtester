from abc import ABC, abstractmethod
from datetime import date
import polars as pl
from backend.models import RealisticBacktestResult, BacktestResult
from backend.utils import build_drop_col_list
from backend.backtest.analysers import BaseAnalyser


class RealisticAnalyser(BaseAnalyser):
    def __init__(self, backtest_results : RealisticBacktestResult):
        super().__init__(backtest_results)
        self.dividends_lf = backtest_results.dividends.lazy()
        self.orders_lf = backtest_results.orders.lazy()

        # Cache enriched cash_lf so daily summary will include order flags
        self.cash_lf = self._enrich_cash_with_order_flags()

        print(backtest_results.data)
        print(backtest_results.calendar)
        print(backtest_results.cash)
        print(backtest_results.holdings)
        print(backtest_results.dividends)
        print(backtest_results.orders)

        print(self.cash_lf.collect())
        print(self.dividends_lf.collect())


    # --- Enrichment helper methods--- # 

    def _enrich_cash_with_order_flags(self) -> pl.LazyFrame:
        """
        Add `did_buy` and `did_sell` flags to the cash LazyFrame based on fulfilled orders.

        Joins cash data with fulfilled order data on `date`, marking buy/sell activity
        with boolean columns. Missing values are filled with `False`.

        Returns:
            pl.LazyFrame: Cash data enriched with buy/sell flags.
        """
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


    def _enrich_dividends_with_holdings(self) -> pl.LazyFrame:
        """
        Join dividends with holdings on date and ticker.

        Returns:
            pl.LazyFrame: Dividends LazyFrame enriched with holdings columns.
        """
        return (
            self.dividends_lf
            .join(self.holdings_lf, on=['date','ticker'], how='left')
        )
    

    # --- Calculation helper methods--- # 

    def _compute_dividend_yields(self, enriched_dividends : pl.LazyFrame) -> pl.LazyFrame:
        """
        Calculate dividend yield as percentage based on base_price.

        Args:
            enriched_dividends (pl.LazyFrame): Dividends LazyFrame with holdings data.

        Returns:
            pl.LazyFrame: LazyFrame with an added 'yield' column.
        """
        return (
            enriched_dividends.with_columns(
                (pl.col('dividend_per_unit') / pl.col('base_price') * 100).alias('yield')
            )
        )
    

    def _compute_cumulative_dividends(self, enriched_dividends : pl.LazyFrame) -> pl.LazyFrame:
        """
        Add year, cumulative all-time dividend, and cumulative yearly dividend and yield columns.

        Args:
            enriched_dividends (pl.LazyFrame): Dividends LazyFrame with yield calculated.

        Returns:
            pl.LazyFrame: LazyFrame joined with cumulative columns for all-time and yearly totals.
        """
        # Add year and all time cumulative column
        alltime_cumulative = (
            enriched_dividends
            .with_columns([
                pl.col('date').dt.year().alias('year'),
                pl.col('total_dividend').cum_sum().alias("cumulative_dividend_alltime")
            ])
        )

        # Calculate cumulative dividend and yield per year
        yearly_cumulative = (
            alltime_cumulative
            .group_by(['year'], maintain_order=True)
            .agg([
                pl.col('date'),
                pl.col('yield').cum_sum().alias('cumulative_yield_year'),
                pl.col('total_dividend').cum_sum().alias('cumulative_dividend_year')
            ])
            .explode(['date','cumulative_yield_year','cumulative_dividend_year'])
        )

        # join all cumulative columns
        return alltime_cumulative.join(yearly_cumulative, on='date', how='left')
    
    
    # --- Compilation methods--- # 

    def _compile_dividend_summary(self) -> pl.LazyFrame:
        """
        Generate a compiled dividend summary as a LazyFrame.

        This method enriches the raw dividends data with holdings information,
        calculates dividend yields, and adds cumulative dividend and yield columns.

        Returns:
            pl.LazyFrame: A lazy dataframe containing enriched dividend data with 
                        units, yield, and cumulative columns.
        """
        # Add units and base_price column to dividend lazyframe
        enriched_divs = self._enrich_dividends_with_holdings()

        # Add yield column
        enriched_divs_with_yield = self._compute_dividend_yields(enriched_divs)

        # Add cumulative columns
        enriched_divs_with_cumulatives = self._compute_cumulative_dividends(enriched_divs_with_yield)

        return enriched_divs_with_cumulatives


    # --- Generating formatted summary dataframes --- #   

    def generate_dividend_summary(self) -> pl.DataFrame:
        """
        Generate a formatted dividend summary as a collected DataFrame.

        Selects and orders key dividend-related columns from the compiled dividend
        summary and collects the lazy frame into an eager DataFrame.

        Returns:
            pl.DataFrame: A dataframe with columns for date, year, ticker, units, 
                        dividend per unit, total dividend, yield, and cumulative metrics.
        """
        COL_ORDER = ['date','year','ticker','units','dividend_per_unit','total_dividend','yield','cumulative_yield_year','cumulative_dividend_year','cumulative_dividend_alltime']
        
        if not hasattr(self, 'dividend_summary_lf'):
            self.dividend_summary_lf = self._compile_dividend_summary()

        div_summary = (self.dividend_summary_lf.select(COL_ORDER).collect())

        return div_summary


    def generate_pivoted_yearly_dividend_summary(self) -> pl.DataFrame:
        """
        Generate a pivoted yearly dividend summary DataFrame.

        Pivots the compiled dividend summary to create a wide-format dataframe 
        indexed by year, with tickers as columns and aggregated dividend and yield 
        metrics as values. Missing values are filled with zeros.

        Returns:
            pl.DataFrame: Pivoted dataframe with yearly dividend and yield metrics 
                        per ticker.
        """
        if not hasattr(self, 'dividend_summary_lf'):
            self.dividend_summary_lf = self._compile_dividend_summary()

        pivot_summary = self.dividend_summary_lf.collect().pivot(
            index='year',
            columns='ticker',
            values=['total_dividend','yield','cumulative_yield_year','cumulative_dividend_year','cumulative_dividend_alltime'],  # or 'cumulative_dividend_year' or 'yield'
            aggregate_function='sum'
        ).fill_null(0)  # fill missing values with zero

        return pivot_summary
    

    def generate_order_summary(self) -> pl.DataFrame:
        pass



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

dividend_summary = test_analyser.generate_dividend_summary()
dividend_pivot_summary = test_analyser.generate_pivoted_yearly_dividend_summary()
print(dividend_summary)
print(dividend_pivot_summary)