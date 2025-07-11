import polars as pl
import warnings
from backend.data_pipeline.ingestors import YFinanceIngestor, CSVIngestor
from backend.data_pipeline.pipelines import BasePipeline
from backend.data_pipeline.processor import PriceProcessor


class PricePipeline(BasePipeline):
    """
    A data pipeline for price data ingestion and processing.

    Supports multiple ingestors (e.g., YFinance, CSV), applies appropriate cleaning,
    and performs forward-filling to produce a backtest-ready price dataset.
    """

    def ingest(self) -> None:
        """
        Run all ingestors and clean the raw data into a unified format.

        CSV filenames should follow the naming pattern 'ticker.csv' to indicate ticker code eg. AAPL.csv

        Raises:
            ValueError: If no data is successfully ingested from any source.
        """
        print("Running price ingestion...")
        cleaned_dfs = []
        for ingestor in self.ingestors:
            try:
                raw_data = ingestor.run()
                if isinstance(ingestor, YFinanceIngestor):
                    cleaned_data = PriceProcessor.clean_yfinance_data(raw_data,ingestor.tickers)
                elif isinstance(ingestor, CSVIngestor):
                    ticker = ingestor.source_path.stem  # ticker is retrieved from the file name without extention ie. AAPL.csv , ticker = AAPL 
                    cleaned_data = PriceProcessor.clean_csv_data(raw_data,ticker)
                else:
                    raise ValueError(f'Unknown ingestor type: {ingestor}')
                cleaned_dfs.append(cleaned_data)

            except Exception as e:
                warnings.warn(f"Error running ingestor : {e}")
                continue
        
        if cleaned_dfs:
            self.cleaned_data = pl.concat(cleaned_dfs)
        else:
            raise ValueError("No data ingested")
    
    def process(self) -> None:
        """
        Apply forward-filling to the cleaned price data.
        """
        print("Running price processing...")
        self.processed_data = PriceProcessor.forward_fill(self.cleaned_data)


    def run(self) -> None:
        """
        Ingest and process the price data in sequence.
        """
        self.ingest()
        self.process()
