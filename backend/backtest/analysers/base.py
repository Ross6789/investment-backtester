from abc import ABC, abstractmethod
from datetime import date
import polars as pl
from backend.models import RealisticBacktestResult, BacktestResult
from backend.utils import build_pivoted_col_names, build_drop_col_list


class BaseAnalyser(ABC):

    def __init__(self, backtest_results : BacktestResult):
        # Convert dataframes to lazyframes
        self.data_lf = backtest_results.data.lazy()
        self.calendar_lf = backtest_results.calendar.lazy()
        self.cash_lf = backtest_results.cash.lazy()
        self.holdings_lf = backtest_results.holdings.lazy()

        print(backtest_results.data)
        print(backtest_results.calendar)
        print(backtest_results.cash)
        print(backtest_results.holdings)

        # Save tickers
        self.tickers = self._get_all_tickers()

        # Update / Create enriched dataframes
        self.holdings_lf = self._add_holding_value()
        self.portfolio_lf = self._generate_portfolio_total_value()
        self.holdings_lf = self._add_portfolio_weighting()


    # --- Helper methods--- # 

    def _get_all_tickers(self) -> list[str]:       
        
        tickers = (
            self.holdings_lf
            .select('ticker')
            .unique()
            .collect()
            .to_series()
            .to_list()
        )
        return tickers
    

    # --- Enriching result dataframes --- # 

    def _add_holding_value(self) -> pl.LazyFrame:       
        
        holdings_with_value = (
            self.holdings_lf
            .with_columns((pl.col("units") * pl.col("base_price")).alias("value"))
        )
        return holdings_with_value


    def _generate_portfolio_total_value(self) -> pl.LazyFrame:

        total_holdings_value = (
            self.holdings_lf
            .group_by('date')
            .agg(pl.sum('value').fill_null(0).alias('total_holding_value'))
        )

        total_portoflio_value = (
            self.cash_lf.join(total_holdings_value, on='date',how='left')
            .with_columns(
                (pl.col('cash_balance')+pl.col('total_holding_value')).alias('total_portfolio_value')
            )
            .select('date','total_holding_value','total_portfolio_value')
        )
        return total_portoflio_value
    

    def _add_portfolio_weighting(self) -> pl.LazyFrame:  
        
        drop_cols = build_drop_col_list(['date'], self.portfolio_lf.collect_schema().names())

        holdings_with_weighting = (
            self.holdings_lf
            .join(self.portfolio_lf, on='date')
            .with_columns((pl.col('value') / pl.col('total_holding_value')*100).alias('portfolio_weighting'))
            .drop(drop_cols)
        )
        return holdings_with_weighting
    

    def _generate_wide_holdings_total_value(self) -> pl.LazyFrame:
       
        PIVOT_VALUES = ["value","portfolio_weighting"]

        wide_holdings_total_value = (
            self.holdings_lf
            .select(["date","ticker"] + PIVOT_VALUES)
            .collect()
            .pivot(values=PIVOT_VALUES, 
                index="date", 
                on="ticker")
                .lazy()
        )

        # Order columns
        pivot_cols = build_pivoted_col_names(self.tickers, PIVOT_VALUES)
        wide_holdings_total_value_ordered = wide_holdings_total_value.select(['date'] + pivot_cols)

        return wide_holdings_total_value_ordered
    
    
    # --- Generating formatted summary dataframes --- # 
    
    def generate_daily_summary(self) -> pl.DataFrame:

        wide_holdings_summary = self._generate_wide_holdings_total_value()

        daily_summary = (
            self.cash_lf
            .join(self.portfolio_lf, on='date', how='left')
            .join(wide_holdings_summary, on='date',how='left')
            .collect()
        )

        return daily_summary


    def generate_holdings_summary(self) -> pl.DataFrame:
       
        PIVOT_VALUES = ['units','native_currency','native_price','exchange_rate','value','portfolio_weighting']

        holdings_fx = (
            self.holdings_lf
            .join(self.data_lf, on=['date','ticker','base_price'])
            .join(self.portfolio_lf, on='date')
            .select(['date','ticker'] + PIVOT_VALUES)
        )
        
        holdings_summary = (
            holdings_fx.collect()
            .pivot(values=PIVOT_VALUES, 
                index="date", 
                on="ticker")
        )

        # Order columns
        pivot_cols = build_pivoted_col_names(self.tickers, PIVOT_VALUES)
        holdings_summary_ordered = holdings_summary.select(['date'] + pivot_cols)
            
        return holdings_summary_ordered


    # --- Calculating overall metrics --- # 

    def calculate_cagr(self):
        pass

    def calculate_drawdown(self):
        pass

    def calculate_best_periods(self):
        pass

    def calculate_worst_periods(self):
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
test_analyser = BaseAnalyser(test_results)

print(test_analyser.holdings_lf.collect())
print(test_analyser.portfolio_lf.collect())

daily_summary = test_analyser.generate_daily_summary()
print(daily_summary)

holdings_summary = test_analyser.generate_holdings_summary()
print(holdings_summary)



