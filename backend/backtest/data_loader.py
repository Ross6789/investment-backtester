import polars as pl
from typing import List
from datetime import date
from backend.utils.metadata import get_valid_benchmark_tickers
import backend.backtest.data_cache as cache
from backend.core.enums import BacktestMode, BaseCurrency


def fetch_filtered_backtest_data(backtest_mode : BacktestMode, base_currency: BaseCurrency, tickers : List[str], start_date: date, end_date: date) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Load and filter backtest and benchmark data for a given set of tickers and date range,
    converting prices to a specified base currency.

    This function performs the following steps:

    1. Loads historical price, FX rate, benchmark, and asset metadata data from cache.
    2. Filters historical price data by ticker and date.
    3. Selects columns based on the chosen backtest mode:
       - BASIC: 'date', 'ticker', 'adj_close', 'is_trading_day'
       - REALISTIC: 'date', 'ticker', 'close', 'is_trading_day', 'dividend'
    4. Renames the price column to 'native_price'.
    5. Retrieves native currency for each ticker from the metadata.
    6. Loads FX rates to convert non-base currency prices to the specified base currency.
    7. Applies a static conversion for GBX (pence) prices to GBP.
    8. Calculates 'base_price' using FX rates where necessary.
    9. Filters benchmark data to include only tickers active for the full date range 
       and already denominated in the base currency.

    Args:
        backtest_mode (BacktestMode): Mode specifying which columns to load and use.
        base_currency (BaseCurrency): The currency to which all prices should be converted.
        tickers (List[str]): List of ticker symbols to include in the backtest.
        start_date (date): Start date for filtering the data.
        end_date (date): End date for filtering the data.

    Returns:
        tuple[pl.DataFrame, pl.DataFrame]: 
            - backtest_data: Polars DataFrame containing at least:
                - 'date': Date of the price data.
                - 'ticker': Ticker symbol.
                - 'native_price': Price in the ticker's original currency (after GBX → GBP conversion if applicable).
                - 'base_price': Price converted to the specified base currency.
                - 'native_currency': Original currency after static conversion.
                - 'exchange_rate': FX rate used for conversion to base currency.
                - Other mode-dependent columns (e.g., 'dividend', 'is_trading_day').
            - benchmark_data: Polars DataFrame containing:
                - 'date': Date of benchmark data.
                - 'ticker': Benchmark ticker symbol.
                - 'price': Price denominated in the base currency.
    
    Note:
        - Both returned DataFrames are collected from lazy mode to eager mode.
        - For very large datasets, returning a LazyFrame may improve performance.
        - Only benchmarks active for the entire period are included.
    """
    historical_prices_lf = cache.get_cached_historical_prices().lazy()
    benchmark_lf = cache.get_cached_benchmarks().lazy()
    fx_lf = cache.get_cached_fx().lazy()
    asset_metadata_lf = cache.get_cached_asset_metadata().lazy()

    # --- PRICE DATA ---

    # Retrieve columns based on backtest mode
    match backtest_mode:
        case BacktestMode.BASIC:
            columns_required = ['date','ticker','adj_close','is_trading_day']
            column_rename_mapping = {'adj_close' : 'native_price'}
        case BacktestMode.REALISTIC:
            columns_required = ['date','ticker','close','is_trading_day','dividend']
            column_rename_mapping = {'close': 'native_price'}
        case _:
            raise ValueError (f'Invalid backtest mode : {backtest_mode}')
    
    # Filter backtest data by dates and tickers
    filtered_price_data = (
        historical_prices_lf
        .filter(
            (pl.col('date')>= start_date)&
            (pl.col('date')<= end_date)&
            (pl.col('ticker').is_in(tickers))
        )
        .select(columns_required)
        .rename(column_rename_mapping)
    )

    # Get native currencies from metadata file
    ticker_currencies = (
        asset_metadata_lf
        .filter(pl.col('ticker').is_in(tickers))
        .select(['ticker','currency'])
    )

    currencies_used = ticker_currencies.select('currency').unique().collect().to_series().to_list()

    # Get FX rates
    fx_rates = (
        fx_lf
        .filter(
            (pl.col('from_currency').is_in(currencies_used))&
            (pl.col('to_currency')== base_currency.value)
        )
    )

    # Join backtest data to metadata and fx rates
    joined_data = (
        filtered_price_data
        .join(ticker_currencies, on='ticker',how='left')
        .join(fx_rates, left_on=['date','currency'], right_on=['date','from_currency'], how='left') # from_currency col on right will simply become currency after join
    )

    # Static conversions : convert from GBX (pence) to GBP
    if 'GBX' in currencies_used:
        joined_data = (
            joined_data.with_columns([
                pl.when(pl.col('currency')=='GBX')
                .then(pl.col('native_price') / 100)
                .otherwise(pl.col('native_price'))
                .alias('native_price'),

                pl.when(pl.col('currency')=='GBX')
                .then(pl.lit('GBP'))
                .otherwise(pl.col('currency'))
                .alias('currency')
            ])
        )

    # Dynamic conversions : calculate base_price using rate if necessary
    converted_backtest_data = (
        joined_data.with_columns([
            # For tickers already in the currency, keep the price otherwise convert
            pl.when(pl.col('currency') == base_currency.value)
            .then(pl.col('native_price'))
            .otherwise(pl.col('native_price') * pl.col('rate'))
            .alias('base_price'),

            pl.col('currency').alias('native_currency'), # rename
            pl.col('rate').alias('exchange_rate') # rename
        ])
        .drop(['currency','to_currency','rate'])
    )

    # --- BENCHMARK DATA ---

    # Determine which benchmark are valid (i.e. active for FULL period)
    valid_benchmark_tickers = get_valid_benchmark_tickers(start_date,end_date)

    # Filter backtest data by dates and tickers
    filtered_benchmark_data = (
        benchmark_lf
        .filter(
            (pl.col('date')>= start_date)&
            (pl.col('date')<= end_date)&
            (pl.col('ticker').is_in(valid_benchmark_tickers))&
            (pl.col('currency')==(base_currency))
        )
        .select('date','ticker','price')
    )

    return converted_backtest_data.collect(), filtered_benchmark_data.collect()  # Convert data from lazy to eager

