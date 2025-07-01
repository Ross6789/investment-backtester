import polars as pl
from pathlib import Path
from typing import List, Dict
from backend.pipelines.ingestors import YFinancePriceIngestor,YFinanceCorporateActionsIngestor, CSVPriceIngestor
from backend.pipelines.processor import PriceProcessor,CorporateActionProcessor
from backend.pipelines.compiler import Compiler

class PricePipeline:
    def __init__(self, ingestors):
        self.ingestors = ingestors  
        self.cleaned_data = None
        self.processed_data = None

    def ingest(self):
        cleaned_dfs = []
        for ingestor in self.ingestors:
            try:
                raw_data = ingestor.run()
                if not raw_data.is_empty():
                    if isinstance(ingestor, YFinancePriceIngestor):
                        cleaned_data = PriceProcessor.clean_yfinance_data(raw_data,ingestor.tickers)
                    elif isinstance(ingestor, CSVPriceIngestor):
                        cleaned_data = PriceProcessor.clean_csv_data(raw_data,ingestor.ticker)
                    else:
                        raise ValueError(f'Unkown ingestor type: {ingestor}')
                    cleaned_dfs.append(cleaned_data)
                else: 
                    print(f"Warning: no data returned from ingestor")
            except Exception as e:
                print(f"Error running ingestor : {e}")
                continue
        
        if cleaned_dfs:
            self.cleaned_data = pl.concat(cleaned_dfs)
        else:
            raise ValueError("No data ingested, therefore file save has been aborted")
    
    def process(self):
        self.processed_data = PriceProcessor.forward_fill(self.cleaned_data)

class CorporateActionPipeline:
    def __init__(self, ingestors):
        self.ingestors = ingestors  
        self.cleaned_data = None
        self.processed_data = None

    def ingest(self):
        cleaned_dfs = []
        for ingestor in self.ingestors:
            try:
                raw_data = ingestor.run()
                if not raw_data.is_empty():
                    if isinstance(ingestor, YFinanceCorporateActionsIngestor):
                        cleaned_data = CorporateActionProcessor.clean_yfinance_data(raw_data,ingestor.tickers)
                    else:
                        raise ValueError(f'Unkown ingestor type: {ingestor}')
                    cleaned_dfs.append(cleaned_data)
                else: 
                    print(f"Warning: no data returned from ingestor")
            except Exception as e:
                print(f"Error running ingestor : {e}")
                continue
        
        if cleaned_dfs:
            self.cleaned_data = pl.concat(cleaned_dfs)
        else:
            raise ValueError("No data ingested, therefore file save has been aborted")
    
    def process(self):
        # No further processing required after clean - if this changes, can call method from CorporateActionProcessor here
        self.processed_data = self.cleaned_data

class MasterPipeline():
    def __init__(self, price_pipeline, action_pipeline, base_save_path):
        self.price_pipeline = price_pipeline
        self.action_pipeline = action_pipeline
        self.base_save_path = Path(base_save_path)
        self.compiled_data = None
    
    # Method to save the data as a partitioned parquet file
    def _save_partitioned_parquet(self, data : pl.DataFrame, directory_save_path: Path):
        try:
            save_dir_base_path = Path(directory_save_path)
            for (ticker,) , ticker_df in data.group_by("ticker"):
                folder = save_dir_base_path / f"ticker={ticker}"
                folder.mkdir(parents=True, exist_ok=True)
                ticker_df.write_parquet(folder / "data.parquet")
            print(f"Data saved to {save_dir_base_path}.") 
        except Exception as e:
            print(f"Error saving parquet data to {directory_save_path}: {e}")


    # Method to save the data as a human readable csv
    def _save_csv(self, data : pl.DataFrame, csv_save_path: Path): 
        try:
            data.write_csv(csv_save_path)
            print(f"Data saved to {csv_save_path}.") 
        except Exception as e:
            print(f"Error saving CSV data to {csv_save_path}: {e}")

    # Run 
    def run(self):
        
        # Run price ingestion
        print("Running price ingestion...")
        self.price_pipeline.ingest()

        # save cleaned price data
        self._save_csv(self.price_pipeline.cleaned_data,self.base_save_path / 'cleaned/csv/prices.csv')
        self._save_partitioned_parquet(self.price_pipeline.cleaned_data,self.base_save_path / 'cleaned/parquet/prices')
        
        # Run price processing
        print("Running price processing...")
        self.price_pipeline.process()

        # save processed price data
        self._save_csv(self.price_pipeline.processed_data,self.base_save_path / 'processed/csv/prices.csv')
        self._save_partitioned_parquet(self.price_pipeline.processed_data,self.base_save_path / 'processed/parquet/prices')

        # Run corporate actions ingestion
        print("Running corporate actions ingestion...")
        self.action_pipeline.ingest()

        # save cleaned corporate action data
        self._save_csv(self.action_pipeline.cleaned_data,self.base_save_path / 'cleaned/csv/corporate_actions.csv')
        self._save_partitioned_parquet(self.action_pipeline.cleaned_data,self.base_save_path / 'cleaned/parquet/corporate_actions')

        # Run corporate action processing
        print("Running corporate action processing...")
        self.action_pipeline.process()

        # Save processed price data
        self._save_csv(self.action_pipeline.processed_data,self.base_save_path / 'processed/csv/corporate_actions.csv')
        self._save_partitioned_parquet(self.action_pipeline.processed_data,self.base_save_path / 'processed/parquet/corporate_actions')

        # Compile
        print("Running data compilation...")
        self.compiled_data = Compiler.compile(self.price_pipeline.processed_data, self.action_pipeline.processed_data)

        # Save compiled data
        self._save_csv(self.compiled_data,self.base_save_path / 'compiled/csv/data.csv')
        self._save_partitioned_parquet(self.compiled_data,self.base_save_path / 'compiled/parquet/data')
        