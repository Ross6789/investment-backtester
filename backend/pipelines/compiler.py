import polars as pl

class Compiler:
    @staticmethod
    def compile(processed_prices: pl.DataFrame, processed_actions: pl.DataFrame) -> pl.DataFrame:
        """
        Join price and corporate actions dataframes on 'date' and 'ticker' columns.

        Args:
            processed_prices (pl.DataFrame): Processed prices DataFrame.
            processed_actions (pl.DataFrame): Processed corporate actions DataFrame.

        Returns:
            pl.DataFrame: Joined DataFrame with prices and corporate actions.
        """
        return processed_prices.join(processed_actions, on=['date','ticker'],how='left')



