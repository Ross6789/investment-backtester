import polars as pl
from backend.models import RealisticBacktestResult
from backend.utils import build_pivoted_col_names, round_dataframe_columns
from backend.backtest.analysers import BaseAnalyser


class RealisticAnalyser(BaseAnalyser):
    """
    Extended analyser for realistic backtest mode.

    Inherits basic analysis capabilities from BaseAnalyser and adds support
    for dividends and order data specific to realistic backtests. Enriches
    cash data with order-related flags for enhanced daily summaries.

    Attributes:
        dividends_lf (pl.LazyFrame): LazyFrame of dividend data.
        orders_lf (pl.LazyFrame): LazyFrame of order data.
        cash_lf (pl.LazyFrame): Enriched cash LazyFrame with order flags.
    """

    def __init__(self, backtest_results : RealisticBacktestResult):
        """
        Initialize the realistic analyser with realistic backtest results.

        Args:
            backtest_results (RealisticBacktestResult): Backtest results including dividends and orders.
        """
        super().__init__(backtest_results)

        #Initialize lazy dataframes (unique to the realistic mode)
        self.dividends_lf = backtest_results.dividends.lazy()
        self.orders_lf = backtest_results.orders.lazy()

        # Cache enriched_cash_lf so daily summary will include order flags
        self.cash_lf = self._enrich_cash_with_order_flags(self.orders_lf, self.cash_lf)


    # --- Enrichment methods--- # 
    
    @staticmethod
    def _enrich_cash_with_order_flags(orders_lf : pl.LazyFrame, cash_lf : pl.LazyFrame) -> pl.LazyFrame:
        """
        Add `did_buy` and `did_sell` flags to the cash LazyFrame based on fulfilled orders.

        Joins cash data with fulfilled order data on `date`, marking buy/sell activity
        with boolean columns. Missing values are filled with `False`.

        Returns:
            pl.LazyFrame: Cash data enriched with buy/sell flags.
        """
        order_flags = (
            orders_lf
            .filter(pl.col('status') == 'fulfilled')
            .select([
                pl.col('date_executed').alias('date'),
                (pl.col('side') == 'buy').alias('did_buy'),
                (pl.col('side') == 'sell').alias('did_sell'),
            ])
        )

        cash_with_flags = (
            cash_lf.join(order_flags,on='date',how='left')
            .fill_null(False)
        )
        return cash_with_flags

    @staticmethod
    def _enrich_dividends_with_units(dividends_lf: pl.LazyFrame, holdings_lf: pl.LazyFrame) -> pl.LazyFrame:
        """
        Join dividends with holdings on date and ticker.

        Returns:
            pl.LazyFrame: Dividends LazyFrame enriched with holdings columns.
        """
        return (
            dividends_lf
            .join(holdings_lf, on=['date','ticker'], how='left')
        )
    

    @staticmethod
    def _enrich_dividends_with_yields(enriched_dividends : pl.LazyFrame) -> pl.LazyFrame:
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
    

    @staticmethod
    def _enrich_orders_with_fx(orders_lf: pl.LazyFrame, data_lf: pl.LazyFrame) -> pl.LazyFrame:
        """
        Join FX data to orders on date, ticker, and base price, 
        adding native currency, native price, and exchange rate columns.

        Args:
            orders_lf (pl.LazyFrame): Orders data.
            data_lf (pl.LazyFrame): FX and price data.

        Returns:
            pl.LazyFrame: Orders enriched with FX-related columns.
        """
        FX_COLS = ['native_currency','native_price','exchange_rate']

        return (
            orders_lf
            .join(
                data_lf, 
                left_on=['date_executed','ticker','base_price'],
                right_on=['date','ticker','base_price'], 
                how='left'
            )
            .select([*orders_lf.collect_schema().keys(), *FX_COLS])
        )
    
    @staticmethod
    def _enrich_orders_with_executed_value(orders_lf : pl.LazyFrame) -> pl.LazyFrame:
        """
        Add 'executed_value' column calculated as units multiplied by base price.

        Args:
            orders_lf (pl.LazyFrame): Orders data.

        Returns:
            pl.LazyFrame: Orders with executed_value column added.
        """
        return (
            orders_lf
            .with_columns(
                (pl.col('units')*pl.col('base_price')).alias('executed_value')
            )
        )
    

    # --- Computation methods--- #     

    def _compute_cumulative_dividends(self, enriched_dividends : pl.LazyFrame) -> pl.LazyFrame:
        """
        Add year, cumulative all-time dividend, and cumulative yearly dividend and yield columns.

        Args:
            enriched_dividends (pl.LazyFrame): Dividends LazyFrame with yield calculated.

        Returns:
            pl.LazyFrame: LazyFrame joined with cumulative columns for all-time and yearly totals.
        """ 
        
        # Add all time cumulative column
        alltime_cumulative = (
            enriched_dividends
            .with_columns([
                pl.col('total_dividend').cum_sum().alias("cumulative_dividend_alltime")
            ])
        )

        # Add year column
        alltime_cumulative = self._enrich_with_year('date',alltime_cumulative)

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
    
    
    # --- LazyFrame compilation --- #   

    def _compile_enriched_dividends(self) -> None:
        """
        Compile enriched dividend data and store it internally.

        This method processes raw dividend data by:
        - Enriching it with units and base prices from holdings.
        - Calculating dividend yield per entry.
        - Computing cumulative dividend income and cumulative yield.

        The resulting enriched LazyFrame is saved to `self.enriched_dividend_lf`.
        """
        enriched_divs = self._enrich_dividends_with_units(self.dividends_lf,self.holdings_lf)
        enriched_divs_with_yield = self._enrich_dividends_with_yields(enriched_divs)
        enriched_divs_with_cumulatives = self._compute_cumulative_dividends(enriched_divs_with_yield)

        self.enriched_dividend_lf = enriched_divs_with_cumulatives
    

    def _compile_enriched_orders(self) -> None:
        """
        Compile enriched orders data and store it internally.

        This method processes raw orders data by:
        - Enriching it with FX rates and data lookups.
        - Calculating executed value per order.

        The resulting enriched LazyFrame is saved to `self.enriched_orders_lf`.
        """
        enriched_orders_fx = self._enrich_orders_with_fx(self.orders_lf,self.data_lf)
        enriched_orders_executed_value = self._enrich_orders_with_executed_value(enriched_orders_fx)

        self.enriched_orders_lf = enriched_orders_executed_value
       
        
    # --- Final report generation --- #

    def generate_dividend_summary(self, price_precision: int | None = None, currency_precision: int | None = None, general_precision: int | None = None) -> pl.DataFrame:
        """
        Generate a formatted dividend summary as a collected DataFrame.

        Selects and orders key dividend-related columns from the compiled dividend summary and collects the lazy frame into an eager DataFrame. 
        Optionally applies rounding to float columns based on their semantic meaning.

        Args:
            price_precision (int | None): Decimal precision for price-like values (e.g., exchange rates). If None, uses default precision.
            currency_precision (int | None): Decimal precision for currency-related values (e.g., total dividends). If None, uses default precision.
            general_precision (int | None): Decimal precision for other float values (e.g., units, yields). If None, uses default precision.

        Returns:
            pl.DataFrame: A rounded DataFrame with columns for date, year, ticker, units, dividend per unit, total dividend, yield, and cumulative metrics.
        """
        COL_ORDER = ['date','year','ticker','units','dividend_per_unit','total_dividend','yield','cumulative_yield_year','cumulative_dividend_year','cumulative_dividend_alltime']
        
        if not hasattr(self, 'enriched_dividend_lf'):
            self._compile_enriched_dividends()

        enriched_divs = (self.enriched_dividend_lf.select(COL_ORDER).collect())

        rounded_summary = round_dataframe_columns(enriched_divs, price_precision, currency_precision, general_precision)
        
        return rounded_summary


    def generate_pivoted_yearly_dividend_summary(self, price_precision: int | None = None, currency_precision: int | None = None, general_precision: int | None = None) -> pl.DataFrame:
        """
        Generate a pivoted yearly dividend summary DataFrame.

        Pivots the compiled dividend summary to create a wide-format dataframe indexed by year, with tickers as columns and aggregated dividend and yield metrics as values. 
        Missing values are filled with zeros. Optionally rounds numeric columns based on their semantic roles.

        Args:
            price_precision (int | None): Decimal precision for price-like values. If None, uses default precision.
            currency_precision (int | None): Decimal precision for currency-related values. If None, uses default precision.
            general_precision (int | None): Decimal precision for other float values. If None, uses default precision.

        Returns:
            pl.DataFrame: Pivoted dataframe with yearly dividend and yield metrics per ticker, rounded appropriately.
        """

        PIVOT_VALUES = ['total_dividend','yield','cumulative_yield_year','cumulative_dividend_year','cumulative_dividend_alltime']

        if not hasattr(self, 'enriched_dividend_lf'):
            self._compile_enriched_dividends()

        pivot_summary = self.enriched_dividend_lf.collect().pivot(
            index='year',
            on='ticker',
            values=PIVOT_VALUES,  
            aggregate_function='sum'
        ).fill_null(0)  # fill missing values with zero
    
        rounded_summary = round_dataframe_columns(pivot_summary, price_precision, currency_precision, general_precision)
        
        return rounded_summary
    
    
    def generate_order_summary(self, price_precision: int | None = None, currency_precision: int | None = None, general_precision: int | None = None) -> pl.DataFrame:
        """
        Return a detailed DataFrame of orders with selected columns.

        Ensures enriched orders are compiled, then selects key columns such as dates, ticker, pricing, units, status, and currency info, collecting the LazyFrame into an eager DataFrame. 
        Numeric columns are optionally rounded based on their semantic roles.

        Args:
            price_precision (int | None): Decimal precision for price-like columns. If None, default precision is used.
            currency_precision (int | None): Decimal precision for currency-related columns. If None, default precision is used.
            general_precision (int | None): Decimal precision for other float columns. If None, default precision is used.

        Returns:
            pl.DataFrame: Order details with specified columns, rounded appropriately.
        """
        COL_ORDER = ['date_placed','date_executed','ticker','target_value','side','status','native_currency','native_price','exchange_rate','base_price','units','executed_value']
        
        if not hasattr (self, 'enriched_orders_lf'):
            self._compile_enriched_orders()

        order_summary = self.enriched_orders_lf.select(COL_ORDER).collect()
    
        rounded_summary = round_dataframe_columns(order_summary, price_precision, currency_precision, general_precision)
        
        return rounded_summary
    

    def generate_pivoted_yearly_order_summary(self, price_precision: int | None = None, currency_precision: int | None = None, general_precision: int | None = None) -> pl.DataFrame:
        """
        Create a yearly summary of fulfilled orders grouped by year, side, and ticker.

        Aggregates counts, average and weighted prices, units, executed values,and price volatility. The results are pivoted with tickers as columns, sorted by year and side, and numeric columns cast appropriately.

        Args:
            price_precision (int | None): Decimal precision for price-like columns. If None, default precision is used.
            currency_precision (int | None): Decimal precision for currency-related columns. If None, default precision is used.
            general_precision (int | None): Decimal precision for other float columns. If None, default precision is used.

        Returns:
            pl.DataFrame: Pivoted yearly order summary with key metrics per ticker.
        """
        PIVOT_VALUES = ['transaction_count', 'total_units', 'average_units_per_transaction', 'total_executed_value', 'average_transaction_value', 'average_transaction_price', 'weighted_average_price', 'price_volatility']

        if not hasattr (self, 'enriched_orders_lf'):
            self._compile_enriched_orders()

        # Filter fulfilled orders only
        fulfilled_orders_lf = self.enriched_orders_lf.filter(pl.col('status')=='fulfilled')

        # Add year column for pivoting
        fulfilled_orders_with_year_lf = self._enrich_with_year('date_executed',fulfilled_orders_lf)

        pivot_summary = (
            fulfilled_orders_with_year_lf.collect()
            .group_by(['year', 'side', 'ticker'])
            .agg([
                pl.col('status').count().alias('transaction_count'),
                pl.col('base_price').mean().alias('average_transaction_price'),
                pl.col('base_price').std().alias('price_volatility'),
                pl.col('units').sum().alias('total_units'),
                pl.col('units').mean().alias('average_units_per_transaction'),
                pl.col('executed_value').sum().alias('total_executed_value'),
            ])
            .with_columns([
                (pl.col('total_executed_value')/pl.col('total_units')).alias('weighted_average_price'),
                (pl.col('total_executed_value')/pl.col('transaction_count')).alias('average_transaction_value'),
            ])
            .pivot(
                index=['year', 'side'],
                on='ticker',
                values=PIVOT_VALUES
            )
            .fill_null(0.0)
            .sort(['year','side'])
        )

        # Cast int columns
        pivot_cols = build_pivoted_col_names(self.tickers, ['transaction_count'])

        cast_pivot_summary = (
            pivot_summary
            .with_columns([
                pl.col('year').cast(pl.Int32).alias('year'),
                *[pl.col(col).cast(pl.Int32).alias(col) for col in pivot_cols]
            ])
        )
    
        rounded_summary = round_dataframe_columns(cast_pivot_summary, price_precision, currency_precision, general_precision)
        
        return rounded_summary


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
print(dividend_summary)

dividend_pivot_summary = test_analyser.generate_pivoted_yearly_dividend_summary()
print(dividend_pivot_summary)

order_summary = test_analyser.generate_order_summary()
print(order_summary)

order_pivot_summary = test_analyser.generate_pivoted_yearly_order_summary()
print(order_pivot_summary)