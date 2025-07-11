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
            from_currency (str): The base currency (e.g. 'USD').
            to_currency (str): The quote currency (e.g. 'GBP').

        Returns:
            pl.DataFrame: Cleaned FX data with standardized column types and currency info.
        """
        print('Starting to clean CSV FX data ...')

        # Convert to polars and cast date column
        try:
            fx_data_pl = pl.from_pandas(raw_data)
            fx_data_pl = fx_data_pl.with_columns(pl.col('date').cast(pl.Date))
        except Exception as e:
            raise Exception(f"Error while converting or casting into polars: {e}")
        
        # Add to_currnecy and from_currency columns
        fx_data_pl = fx_data_pl.with_columns([
            pl.lit(from_currency.upper()).alias('from_currency'),
            pl.lit(to_currency.upper()).alias('to_currency')
        ])
        
        print('CSV FX data cleaned.')
        return fx_data_pl.sort('date')
    

    @staticmethod
    def generate_inverse_rates(cleaned_fx: pl.DataFrame) -> pl.DataFrame:
        """
        Generates inverse FX rates (e.g. USD/GBP from GBP/USD).

        Args:
            cleaned_fx (pl.DataFrame): Cleaned FX data with 'from_currency', 'to_currency', 'rate', and 'date'.

        Returns:
            pl.DataFrame: Combined FX data including original and inverse rates.
        """
        print('Generating reverse FX rates ...')

        inverted_fx = (
            cleaned_fx
            .with_columns([
                pl.col("from_currency").alias("to"),
                pl.col("to").alias("from"),
                (1 / pl.col("rate")).alias("rate")
            ])
            .select([
                pl.col("to").alias("to_currency"),
                pl.col("from").alias("from_currency"),
                "rate",
                "date"
            ])
        )

        # Combine original and inverted
        fx_full = pl.concat([cleaned_fx, inverted_fx]).unique()
        
        print('Reverse FX rates generated.')
        return fx_full.sort(['from_currency','to_currency','date'])
