import polars as pl
import pandas as pd

class FXProcessor:
    """
    Handles cleaning and transformation of raw fx (foreign exchange) rate data for use in backtesting pipelines.
    """

    @staticmethod
    def clean_csv_data(raw_data: pd.DataFrame, from_currency : str, to_currency : str) -> pl.DataFrame:
        """
        Cleans raw FX rate data from a CSV source.

        Args:
            raw_data (pd.DataFrame): Raw FX rate data with 'date' and 'rate' columns.
            from_currency (str): The base currency (e.g., 'USD').
            to_currency (str): The quote currency (e.g., 'GBP').

        Returns:
            pl.DataFrame: Cleaned FX data with standardized column types and currency info.

        Raises:
            RuntimeError: If conversion to Polars or casting fails.
        """
        print('Starting to clean CSV FX data ...')

        # Convert to polars and cast date column
        try:
            fx_data_pl = pl.from_pandas(raw_data)
            fx_data_pl = fx_data_pl.with_columns(pl.col('date').cast(pl.Date))
        except Exception as e:
            raise RuntimeError(f"Error while converting or casting into polars: {e}")
        
        # Add to_currency and from_currency columns
        fx_data_pl = (
            fx_data_pl
            .with_columns([
                pl.lit(from_currency.upper()).alias('from_currency'),
                pl.lit(to_currency.upper()).alias('to_currency')
            ])
            .select('date','from_currency','to_currency','rate')
        )
        
        print('CSV FX data cleaned.')
        return fx_data_pl.sort('date')
    

    @staticmethod
    def forward_fill(cleaned_fx: pl.DataFrame) -> pl.DataFrame:
        """
        Forward-fill missing fx data for each currency pair over its full date range.

        Creates a complete daily date range per currency-pair, joins with the input fx data,
        and forward-fills missing rates.

        Args:
            cleaned_fx (pl.DataFrame): Polars DataFrame with columns ['date', 'from_currency', 'to_currency', 'rate'] containing cleaned fx data.

        Returns:
            pl.DataFrame: Polars DataFrame with no missing dates and forward-filled rates.
        """
        print('Forward filling fx data ...')

        # Determine date ranges for each currency pair
        currency_pair_date_ranges = (
            cleaned_fx.group_by(['from_currency','to_currency']).agg([
                pl.col('date').min().alias('min_date'),
                pl.col('date').max().alias('max_date')
            ])
        )

        # Create dataframe of all dates for each currency-pair using its daterange
        date_dfs = []
        for from_currency, to_currency, min_date, max_date in currency_pair_date_ranges.iter_rows():
            date_df = pl.DataFrame({'date':pl.date_range(min_date,max_date,interval='1d', eager=True), 'from_currency':from_currency, 'to_currency':to_currency})
            date_dfs.append(date_df)
        full_date_df = pl.concat(date_dfs)

        # Join full dates df to prices df and forward fill
        filled_fx_rates = (
            full_date_df.join(cleaned_fx, on=['date','from_currency','to_currency'], how='left')
            .sort(['from_currency','to_currency','date'])
            .with_columns(
                pl.col('rate').fill_null(strategy='forward')
            )
        )
        print('FX data forward filled.')
        return filled_fx_rates 


    @staticmethod
    def generate_inverse_rates(filled_fx: pl.DataFrame) -> pl.DataFrame:
        """
        Generates inverse FX rates (e.g. USD/GBP from GBP/USD).

        Args:
            filled_fx (pl.DataFrame): FX data with 'from_currency', 'to_currency', 'rate', and 'date'.

        Returns:
            pl.DataFrame: Combined FX data including original and inverse rates.
        """
        print('Generating reverse FX rates ...')

        inverted_fx = (
            filled_fx
            .with_columns([
                pl.col("from_currency").alias("temp_to"),
                pl.col("to_currency").alias("temp_from"),
                (1 / pl.col("rate")).alias("rate")
            ])
            .select([
                "date",
                pl.col("temp_from").alias("from_currency"),
                pl.col("temp_to").alias("to_currency"),
                "rate"
            ])
        )

        # Combine original and inverted
        fx_full = pl.concat([filled_fx, inverted_fx]).unique()
        
        print('Reverse FX rates generated.')
        return fx_full.sort(['from_currency','to_currency','date'])

