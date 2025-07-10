from typing import Iterator
import yfinance as yf
import warnings
import pandas as pd
from datetime import date
from time import sleep

class YFinanceIngestor:
    """
    A class to ingest financial data from Yahoo Finance in batches.

    This class downloads price data and optionally corporate actions data
    for a list of ticker symbols over a specified date range, handling batch
    downloads to avoid API rate limits.

    Attributes:
        tickers (list[str]): List of ticker symbols to download data for.
        batch_size (int): Number of tickers to process per batch.
        start_date (date): Start date for data download.
        end_date (date): End date for data download.
        include_actions (bool): Whether to include corporate actions data.
    """
    def __init__(self, tickers: list[str], batch_size: int, start_date: date, end_date: date, include_actions : bool = False):
        """
        Initializes the YFinanceIngestor with tickers, batch size, date range, and options.

        Args:
            tickers (list[str]): List of ticker symbols to fetch data for.
            batch_size (int): Number of tickers to download per batch; must be > 0.
            start_date (date): Start date of the data range.
            end_date (date): End date of the data range; must be >= start_date.
            include_actions (bool, optional): Whether to include corporate actions data. Defaults to False.

        Raises:
            TypeError: If batch_size is not an integer.
            ValueError: If batch_size < 1 or start_date is after end_date.
        """
        if not isinstance(batch_size, int):
            raise TypeError("Batch size must be an integer")
        if batch_size < 1:
            raise ValueError("Batch size must be greater than zero")
        if start_date > end_date:
            raise ValueError("Start date must be after the end date")
        self.tickers = tickers
        self.start_date = start_date
        self.batch_size = batch_size
        self.end_date = end_date  
        self.include_actions = include_actions


    def _batch_tickers(self) -> Iterator[list[str]]:
        """
        Yield successive batches of tickers from the ticker list.

        This helps to avoid API rate limits by downloading data in manageable chunks.

        Returns:
            Iterator[list[str]]: An iterator that yields batches of tickers as lists.
        """
        for i in range(0, len(self.tickers), self.batch_size):
            yield self.tickers[i:i + self.batch_size]

    def _download_data(self, tickers_batch: list[str]) -> pd.DataFrame:
        """
        Download price and optionally corporate actions data for a batch of tickers.

        Uses the yfinance API to download data within the specified date range. 
        If the downloaded data is empty, a warning is issued.

        Args:
            tickers_batch (list[str]): A list of ticker symbols to download data for.

        Returns:
            pd.DataFrame: The downloaded data as a pandas DataFrame.

        Raises:
            RuntimeError: If there is an error during download.
        """
        print(f"Starting download...")
        try:
            raw_data = yf.download(tickers_batch, self.start_date, self.end_date, auto_adjust= False, actions=self.include_actions)
            
            print('Download complete.')

            if raw_data.empty:
                warnings.warn(f"Downloaded data is empty.")

            return raw_data
        
        except Exception as e:
            raise RuntimeError(f"Failed download : {e}")

    # Method to run internal methods in order
    def run(self) -> pd.DataFrame:
        """
        Execute the full download process over all batches of tickers.

        Iterates over batches of tickers, downloads their data, and concatenates
        all batch results into a single DataFrame. If all batches fail, raises an error.

        Returns:
            pd.DataFrame: Concatenated DataFrame containing all downloaded data batches.

        Raises:
            RuntimeError: If all batches fail to download data or concatenation fails.
        """
        all_data = []

        for batch in self._batch_tickers():
            print(f"Processing batch : {batch}")
            try:
                batch_data = self._download_data(batch)
                if batch_data is not None and not batch_data.empty:
                    all_data.append(batch_data)
            except Exception as e:
                warnings.warn(e)
                continue
            sleep(2)  # add 2 second delay to prevent api restrictions
        
        if not all_data:
            raise RuntimeError("All downloads failed - no data to return")
        
        try:
            return pd.concat(all_data)
        except Exception as e:
            raise RuntimeError(f"Error during batch concatenation: {e}")
