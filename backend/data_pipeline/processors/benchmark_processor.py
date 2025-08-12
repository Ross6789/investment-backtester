import polars as pl
from backend.core.paths import get_fx_data_path, get_benchmark_metadata_csv_path

class BenchmarkProcessor:
    """
    Handles cleaning and transformation of benchmark data for use in backtesting pipelines.
    """
    @staticmethod
    def convert_prices_to_all_base_currencies(benchmark_prices_df: pl.DataFrame) -> pl.DataFrame:
        """
        Convert the processed benchmark price data into all desired base currencies.
        This should update self.processed_data with the currency-adjusted prices.
        """

        benchmark_metadata_path = get_benchmark_metadata_csv_path()
        fx_data_path = get_fx_data_path()

        # Scan benchmark metadata file to find get ticker - currency pairs
        benchmark_currency_lf = pl.scan_csv(benchmark_metadata_path).select(['ticker','currency'])

        # Clean existing benchmark data
        benchmark_lf = benchmark_prices_df.select(['date','ticker','adj_close']).rename({'adj_close':'native_price'}).lazy()
        
        # Add native currency to benchmark data
        benchmark_with_currency_lf = benchmark_lf.join(benchmark_currency_lf, on='ticker', how='left').rename({'currency':'native_currency'})

        # Load FX rates data
        fx_rates_df = pl.scan_parquet(fx_data_path)

        # Build a LazyFrame of all currencies available to convert to:
        all_currencies_lf = fx_rates_df.select('to_currency').unique().rename({'to_currency': 'currency'})

        # Cross join to get all combos - need to filter out rows were
        benchmark_required_currencies_lf = benchmark_with_currency_lf.join(all_currencies_lf, how='cross')
        benchmark_required_currencies_lf = benchmark_required_currencies_lf.filter(
            pl.col('native_currency') != pl.col('currency')
        )       

        # Join FX rates to get conversion rate from native currency to target currency on each date
        conversion_lf = benchmark_required_currencies_lf.join(
            fx_rates_df,
            left_on=['date', 'native_currency', 'currency'], 
            right_on=['date', 'from_currency', 'to_currency'],
            how='left'
        )

        # Calculate converted price
        converted_lf = conversion_lf.with_columns([
            (pl.col('native_price') * pl.col('rate')).alias('price'),
        ]).select(['ticker', 'date', 'currency', 'price'])

        # Add the native price rows (price in native currency = native_price)
        native_prices = benchmark_with_currency_lf.select([
            'ticker',
            'date',
            pl.col('native_currency').alias('currency'),
            pl.col('native_price').alias('price')
        ])

        # Combine converted prices + native prices
        final_lf = pl.concat([converted_lf, native_prices])

        # Remove any rows with missing price eg. euro conversion before 1999 inception
        final_lf = final_lf.filter(pl.col('price').is_not_null())

        return final_lf.sort(['ticker', 'date', 'currency']).collect()