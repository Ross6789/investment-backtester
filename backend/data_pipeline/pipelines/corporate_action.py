import polars as pl
import warnings
from backend.data_pipeline.ingestors import YFinanceIngestor, CSVIngestor
from backend.data_pipeline.pipelines import BasePipeline
from backend.data_pipeline.processor import CorporateActionProcessor


class CorporateActionPipeline(BasePipeline):
    """
    Pipeline for ingesting and cleaning corporate action data.

    Currently supports ingestion from YFinance-based ingestors.
    Applies appropriate cleaning and stores the cleaned data.
    """
    
    def ingest(self):
        """
        Run all configured ingestors and clean the corporate action data.

        Raises:
            ValueError: If no data is successfully ingested.
        """
        cleaned_dfs = []
        for ingestor in self.ingestors:
            try:
                raw_data = ingestor.run()
                if isinstance(ingestor, YFinanceIngestor):
                    cleaned_data = CorporateActionProcessor.clean_yfinance_data(raw_data,ingestor.tickers)
                else:
                    raise ValueError(f'Unkown ingestor type: {ingestor}')
                cleaned_dfs.append(cleaned_data)

            except Exception as e:
                warnings.warn(f"Error running ingestor : {e}")
                continue
        
        if cleaned_dfs:
            self.cleaned_data = pl.concat(cleaned_dfs)
        else:
            raise ValueError("No data ingested, therefore file save has been aborted")
    
    
    def process(self):
        """
        Pass-through method for corporate action data.

        No additional processing is required after initial cleaning,
        so this method simply copies `cleaned_data` to `processed_data`.
        """
        self.processed_data = self.cleaned_data

