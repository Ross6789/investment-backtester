from abc import ABC, abstractmethod
from typing import List, Iterator
import yfinance as yf
import pandas as pd
from pathlib import Path
from datetime import date
from time import sleep

class BaseYFinanceIngestor(ABC):
    def __init__(self, tickers: List[str], batch_size: int, start_date: date, end_date: date):
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

    # Method to batch the tickers to prevent API ban or freezing (_ is convention for private methods)
    def _batch_tickers(self) -> Iterator[List[str]]:
        for i in range(0, len(self.tickers), self.batch_size):
            yield self.tickers[i:i + self.batch_size]

    # Method to run internal methods in order
    def run(self) -> pd.DataFrame:
        batch_data = []
        for batch in self._batch_tickers():
            print(f"Downloading batch {batch}")
            try:
                batch_data.append(self.download_data(batch))
            except Exception as e:
                print (f"Error downloading batch {batch} : {e}")
                continue
            # add 2 second delay to prevent api restrictions
            sleep(2)
        try:
            combined_data = pd.concat(batch_data)
        except Exception as e:
            raise RuntimeError(f"Error during batch concatenation: {e}")
        return combined_data
    
    @abstractmethod
    def download_data(self, tickers_batch: List[str]) -> pd.DataFrame:
        pass  
    
class YFinancePriceIngestor(BaseYFinanceIngestor):

    # Method to download data using yfinance
    def download_data(self, tickers_batch: List[str]) -> pd.DataFrame:
        print('Starting download...')
        try:
            raw_data = yf.download(tickers_batch, self.start_date, self.end_date, auto_adjust= False)
        except Exception as e:
            raise RuntimeError(f"Failed to download data for batch {tickers_batch}: {e}")
        
        if raw_data.empty:
            raise ValueError(f"Downloaded data for batch {tickers_batch} is empty.")
        print('Download complete.')
        return raw_data
    
class YFinanceCorporateActionsIngestor(BaseYFinanceIngestor):
    # Method to download data using yfinance
    def download_data(self, tickers_batch: List[str]) -> pd.DataFrame:
        print('Starting download...')
        try:
            raw_data = yf.download(tickers_batch, self.start_date, self.end_date, auto_adjust= False, actions=True)
        except Exception as e:
            raise RuntimeError(f"Failed to download data for batch {tickers_batch}: {e}")
        
        if raw_data.empty:
            raise ValueError(f"Downloaded data for batch {tickers_batch} is empty.")
        print('Download complete.')
        return raw_data
    
class CSVPriceIngestor:
    def __init__(self,ticker: str,source_path: Path, start_date: date, end_date: date):
        if start_date and end_date:
            if start_date > end_date:
                raise ValueError("Start date must be after the end date")
        self.ticker = ticker
        self.source_path = source_path
        self.start_date = start_date
        self.end_date = end_date

    # Method to download data from csv
    def run(self) -> pd.DataFrame:
        print(f"Reading file : {self.source_path}...")
        all_data = pd.read_csv(self.source_path, parse_dates=['date'], dayfirst=True)
        # Convert start and end date to pandas for comparison
        start_date_pd = pd.Timestamp(self.start_date)
        end_date_pd = pd.Timestamp(self.end_date)
        # Ingest only rows within the date range
        raw_data = all_data[(all_data['date'] >= start_date_pd) & (all_data['date'] <= end_date_pd)]
        print("Read complete.")
        if raw_data.empty:
            raise ValueError(f"No data in {self.source_path} within the date range.")
        return raw_data

