import polars as pl
from pathlib import Path
from backend.data_pipeline.ingestors import YFinancePriceIngestor,YFinanceCorporateActionsIngestor, CSVIngestor
from backend.data_pipeline.processor import PriceProcessor,CorporateActionProcessor, FXProcessor
from backend.data_pipeline.compiler import Compiler

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
                if not raw_data.empty:
                    if isinstance(ingestor, YFinancePriceIngestor):
                        cleaned_data = PriceProcessor.clean_yfinance_data(raw_data,ingestor.tickers)
                    elif isinstance(ingestor, CSVIngestor):
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
                if not raw_data.empty:
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

class FXPipeline:
    def __init__(self, ingestors):
        self.ingestors = ingestors  
        self.cleaned_data = None
        self.processed_data = None

    def ingest(self):
        cleaned_dfs = []
        for ingestor in self.ingestors:
            try:
                raw_data = ingestor.run()
                if not raw_data.empty:
                    if isinstance(ingestor, CSVIngestor):
                        cleaned_data = FXProcessor.clean_fx_data(raw_data, ingestor.source_path)
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
        self.processed_data = FXProcessor.fill_reversed_fx(self.cleaned_data)


class MasterPipeline:

    def __init__(self, price_pipeline : PricePipeline, action_pipeline : CorporateActionPipeline, fx_pipeline : FXPipeline,  base_save_path : Path):
        """
        Initialize the master pipeline to coordinate all sub-pipelines:
        - Price data
        - Corporate action data
        - FX rate data

        Args:
            price_pipeline (PricePipeline): Pipeline for handling price data.
            action_pipeline (CorporateActionPipeline): Pipeline for corporate actions.
            fx_pipeline (FXPipeline): Pipeline for FX rates.
            base_save_path (Path): Root path for saving pipeline outputs.
        """
        self.price_pipeline = price_pipeline
        self.action_pipeline = action_pipeline
        self.fx_pipeline = fx_pipeline
        self.base_save_path = Path(base_save_path)
        self.compiled_data = None
    

    def _save_partitioned_parquet(self, data : pl.DataFrame, directory_save_path: Path) -> None:
        """
        Save a Polars DataFrame as a partitioned Parquet directory by ticker.

        Args:
            data (pl.DataFrame): DataFrame to save.
            directory_save_path (Path): Directory to write partitioned Parquet files to.
        """
        try:
            for (ticker,) , ticker_df in data.group_by("ticker"):
                folder = directory_save_path / f"ticker={ticker}"
                folder.mkdir(parents=True, exist_ok=True)
                ticker_df.write_parquet(folder / "data.parquet")
            print(f"Data saved to {directory_save_path}.") 
        except Exception as e:
            print(f"Error saving parquet data to {directory_save_path}: {e}")


    def _save_regular_parquet(self, data : pl.DataFrame, save_path: Path) -> None:
        """
        Save a Polars DataFrame as a single flat Parquet file.

        Args:
            data (pl.DataFrame): DataFrame to save.
            save_path (Path): Path to save the Parquet file.
        """
        try:
            data.write_parquet(save_path)
            print(f"Data saved to {save_path}.") 
        except Exception as e:
            print(f"Error saving parquet data to {save_path}: {e}")


    def _save_csv(self, data : pl.DataFrame, save_path: Path) -> None: 
        """
        Save a Polars DataFrame as a CSV file.

        Args:
            data (pl.DataFrame): DataFrame to save.
            save_path (Path): Path to save the CSV file.
        """
        try:
            data.write_csv(save_path)
            print(f"Data saved to {save_path}.") 
        except Exception as e:
            print(f"Error saving CSV data to {save_path}: {e}")


    def _save_csv_only(self, name: str, data: pl.DataFrame, stage: str) -> None: 
        """
        Save only a CSV file for inspection purposes.

        Args:
            name (str): Dataset name (e.g., 'prices').
            data (pl.DataFrame): DataFrame to save.
            stage (str): Stage of the pipeline ('cleaned', 'processed').
        """
        save_path = self.base_save_path / f"{stage}/csv/{name}.csv"
        self._save_csv(data, save_path)


    def _save_csv_and_parquet(self, name: str, data: pl.DataFrame, stage: str, partitioned: bool = False) -> None:
        """
        Save both CSV and Parquet formats for final datasets.

        Args:
            name (str): Dataset name (e.g., 'data', 'fx').
            data (pl.DataFrame): DataFrame to save.
            stage (str): Stage of the pipeline ('processed', 'compiled').
            partitioned (bool): Whether to save as partitioned Parquet.
        """
        csv_path = self.base_save_path / f"{stage}/csv/{name}.csv"
        partitioned_parquet_path = self.base_save_path / f"{stage}/parquet/{name}" 
        regular_parquet_path = self.base_save_path / f"{stage}/parquet/{name}.parquet"
        
        self._save_csv(data, csv_path)

        if partitioned:
            self._save_partitioned_parquet(data, partitioned_parquet_path)
        else:
            self._save_regular_parquet(data, regular_parquet_path)

    # Run 
    def run(self) -> None:
        """
        Run the entire master pipeline in sequence:
        - Ingest and process price and corporate action data.
        - Compile them into a single backtest-ready dataset.
        - Ingest and process FX data separately.
        - Save interim CSVs for human inspection.
        - Save final data (compiled prices and FX rates) in both CSV and Parquet formats.
        """
        
        # --- PRICE PIPELINE ---
         
        print("Running price ingestion...")
        self.price_pipeline.ingest()
        self._save_csv_only('prices',self.price_pipeline.cleaned_data,'cleaned')

        print("Running price processing...")
        self.price_pipeline.process()
        self._save_csv_only('prices',self.price_pipeline.processed_data,'processed')

        # --- CORPORATE ACTION PIPELINE ---

        print("Running corporate actions ingestion...")
        self.action_pipeline.ingest()
        self._save_csv_only('corporate_actions',self.action_pipeline.cleaned_data,'cleaned')
       
        print("Running corporate action processing...")
        self.action_pipeline.process()
        self._save_csv_only('corporate_actions',self.action_pipeline.processed_data,'processed')

        # --- COMPILE TO FINAL BACKTEST DATASET ---

        print("Running backtest data compilation...")
        self.compiled_data = Compiler.compile(self.price_pipeline.processed_data, self.action_pipeline.processed_data)
        self._save_csv_and_parquet('data',self.compiled_data,'compiled',True)

        # --- FX PIPELINE ---
        
        print("Running fx rate ingestion...")
        self.fx_pipeline.ingest()
        self._save_csv_only('fx',self.fx_pipeline.cleaned_data,'cleaned')
        
        print("Running fx rate processing...")
        self.fx_pipeline.process()
        self._save_csv_and_parquet('fx',self.fx_pipeline.processed_data,'processed',False)