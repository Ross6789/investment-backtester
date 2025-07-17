from abc import ABC
import polars as pl
from backend.core.models import BacktestResult
from backend.utils.reporting import generate_suffixed_col_names, build_drop_col_list


class BaseAnalyser(ABC):
    """
    Basic analyser class for processing backtest results.

    Initializes lazy dataframes for key backtest components and caches unique tickers for efficient access in analysis methods.

    Attributes:
        data_lf (pl.LazyFrame): LazyFrame of backtest price data.
        calendar_lf (pl.LazyFrame): LazyFrame of the calendar data.
        cash_lf (pl.LazyFrame): LazyFrame of cash balances over time.
        holdings_lf (pl.LazyFrame): LazyFrame of holdings over time.
        tickers (list[str]): List of unique ticker symbols from holdings.
    """

    def __init__(self, backtest_results : BacktestResult):
        """
        Initialize the BaseAnalyser with backtest result data.

        Args:
            backtest_results (BacktestResult): Object containing backtest dataframes.
        """
        # Initialize lazy dataframes
        self.data_lf = backtest_results.data.lazy()
        self.calendar_lf = backtest_results.calendar.lazy()
        self.cash_lf = backtest_results.cash.lazy()
        self.holdings_lf = backtest_results.holdings.lazy()

        # Cache tickers for use in future methods
        self.tickers = self._get_all_tickers()


    def _get_all_tickers(self) -> list[str]:       
        """
        Retrieve a list of unique ticker symbols from the holdings data.

        Returns:
            list[str]: Unique tickers present in holdings_lf.
        """
        return (
            self.holdings_lf
            .select('ticker')
            .unique()
            .collect()
            .to_series()
            .to_list()
        )
 

    # --- Enrichment methods --- # 

    @staticmethod
    def _enrich_with_year(date_col: str, lf: pl.LazyFrame) -> pl.LazyFrame:
        """
        Adds a 'year' column to the given LazyFrame based on the specified date column.

        Args:
            date_col (str): Name of the date column to extract the year from.
            lf (pl.LazyFrame): A Polars LazyFrame containing a 'date' column of type Date.

        Returns:
            pl.LazyFrame: A LazyFrame with an additional 'year' column.
        """
        return lf.with_columns( pl.col(date_col).dt.year().alias('year'))
    

    @staticmethod
    def _enrich_holdings_with_values(holdings_lf: pl.LazyFrame) -> pl.LazyFrame:  
        """
        Calculate the total value of each holding by multiplying units by base price.

        Args:
            holdings_lf (pl.LazyFrame): Holdings data including 'units' and 'base_price' columns.

        Returns:
            pl.LazyFrame: Holdings with an added 'value' column representing total holding value.
        """     
        return (
            holdings_lf
            .with_columns((pl.col("units") * pl.col("base_price")).alias("value"))
        )

    @staticmethod
    def _enrich_holdings_with_portfolio_weighting(holdings_lf: pl.LazyFrame, portfolio_lf: pl.LazyFrame) -> pl.LazyFrame:  
        """
        Add portfolio weighting (%) to each holding based on its value proportion of total holdings.

        Args:
            holdings_lf (pl.LazyFrame): Holdings data with 'value' per asset per date.
            portfolio_lf (pl.LazyFrame): Portfolio totals data with 'total_holding_value' per date.

        Returns:
            pl.LazyFrame: Holdings enriched with 'portfolio_weighting' column representing the proportion of each holding relative to total holdings on that date.
        """
        drop_cols = build_drop_col_list(['date'], portfolio_lf.collect_schema().keys())

        holdings_with_weighting = (
            holdings_lf
            .join(portfolio_lf, on='date')
            .with_columns((pl.col('value') / pl.col('total_holding_value')).alias('portfolio_weighting'))
            .drop(drop_cols)
        )
        return holdings_with_weighting
    

    # --- Computation methods--- #    

    @staticmethod
    def _compute_portfolio_totals(holdings_lf: pl.LazyFrame, cash_lf: pl.LazyFrame) -> pl.LazyFrame:
        """
        Calculate daily portfolio totals by combining holdings and cash balances.

        Args:
            holdings_lf (pl.LazyFrame): Holdings data with 'value' column per asset per date.
            cash_lf (pl.LazyFrame): Cash balance data per date.

        Returns:
            pl.LazyFrame: DataFrame with columns 'date', 'total_holding_value', and 'total_portfolio_value'.
        """
        # Add total holdings value column
        total_holdings_value = (
            holdings_lf
            .group_by('date')
            .agg(pl.sum('value').fill_null(0).alias('total_holding_value'))
        )

        # Add total portfolio value column
        total_portoflio_value = (
            cash_lf.join(total_holdings_value, on='date',how='left')
            .with_columns(
                (pl.col('cash_balance')+pl.col('total_holding_value')).alias('total_portfolio_value')
            )
            .select('date','total_holding_value','total_portfolio_value')
        )
        return total_portoflio_value
    

    # --- Post-pivot calculation expressions--- #    

    @staticmethod
    def _daily_gain_expr() -> pl.Expr:
        """
        Return an expression to compute the absolute daily gain in total portfolio value.

        This calculates the difference between the current and previous day's
        'total_portfolio_value'.

        Returns:
            pl.Expr: An expression representing the 'daily_gain' column.
        """
        return pl.col("total_portfolio_value").diff().alias("daily_gain")
    

    @staticmethod
    def _daily_return_expr() -> pl.Expr:
        """
        Return an expression to compute the percentage daily return in total portfolio value.

        This calculates the return as the relative change between the current and previous day's
        'total_portfolio_value'.

        Returns:
            pl.Expr: An expression representing the 'daily_return' column.
        """
        return ((pl.col("total_portfolio_value") / pl.col("total_portfolio_value").shift(1)) - 1).alias("daily_return")


    # --- LazyFrame compilation --- #   

    def _compile_enriched_data(self) -> None:
        """
        Compile enriched holdings and portfolio data.

        Enhances holdings with value calculations, computes portfolio totals, and enriches holdings with portfolio weighting, storing results as instance attributes.
        """
        holdings_with_values = self._enrich_holdings_with_values(self.holdings_lf)

        self.portfolio_lf = self._compute_portfolio_totals(holdings_with_values,self.cash_lf)

        self.enriched_holdings_lf = self._enrich_holdings_with_portfolio_weighting(holdings_with_values,self.portfolio_lf)


    # --- Report formatting --- #

    def _format_wide_holdings_summary(self) -> pl.LazyFrame:
        """
        Pivot enriched holdings data to wide format with 'value' and 'portfolio_weighting' per ticker.

        Collects and pivots data on 'date' and 'ticker', then orders columns consistently.

        Returns:
            pl.LazyFrame: Pivoted holdings data in wide format with ordered columns.
        """
        PIVOT_VALUES = ["value","portfolio_weighting"] 

        wide_holdings_total_value = (
            self.enriched_holdings_lf
            .select(["date","ticker", *PIVOT_VALUES])
            .collect()
            .pivot(values=PIVOT_VALUES, 
                index="date", 
                on="ticker")
                .lazy()
        )

        # Order columns
        pivot_cols = generate_suffixed_col_names(PIVOT_VALUES, self.tickers)
        wide_holdings_total_value_ordered = wide_holdings_total_value.select(['date'] + pivot_cols)

        return wide_holdings_total_value_ordered
    

    # --- Final report generation --- #
    
    def generate_daily_summary(self) -> pl.DataFrame:
        """
        Generate a daily summary by joining cash, portfolio, and holdings data.

        Ensures enriched holdings data is compiled, formats holdings in wide format, then joins cash and portfolio data on date.

        Returns:
            pl.DataFrame: Combined daily summary with cash, portfolio, and holdings info.
        """
        if not hasattr(self, 'enriched_holdings_lf'):
            self._compile_enriched_data()

        wide_holdings_summary = self._format_wide_holdings_summary()

        daily_summary = (
            self.cash_lf
            .join(self.portfolio_lf, on='date', how='left')
            .join(wide_holdings_summary, on='date',how='left')
            .fill_null(0)
            .collect()
        )
        
        # Apply post-pivot calculations
        daily_summary = daily_summary.with_columns([
            self._daily_gain_expr(),
            self._daily_return_expr()
        ])

        return daily_summary


    def generate_holdings_summary(self) -> pl.DataFrame:
        """
        Generate a pivoted summary of holdings with FX and portfolio data.

        Joins enriched holdings with FX and portfolio data, then pivots specified value columns by date and ticker to create a wide-format summary. 

        Returns:
            pl.DataFrame: Pivoted holdings summary with one column per (ticker, value type) combination, plus the date column.
        """
        PIVOT_VALUES = ['units','native_currency','native_price','exchange_rate','value','portfolio_weighting']

        if not hasattr(self, 'enriched_holdings_lf'):
            self._compile_enriched_data()

        holdings_fx = (
            self.enriched_holdings_lf
            .join(self.data_lf, on=['date','ticker','base_price'])
            .join(self.portfolio_lf, on='date')
            .select(['date','ticker', *PIVOT_VALUES])
        )
        
        holdings_summary = (
            holdings_fx.collect()
            .pivot(values=PIVOT_VALUES, 
                index="date", 
                on="ticker")
        )

        # Order columns
        pivot_cols = generate_suffixed_col_names(PIVOT_VALUES, self.tickers)
        holdings_summary_ordered = holdings_summary.select(['date', *pivot_cols])
        
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







