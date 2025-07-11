import polars as pl
import warnings
from pathlib import Path
from backend.utils import save_csv, save_regular_parquet,save_partitioned_parquet
from backend.data_pipeline.pipelines import PricePipeline, CorporateActionPipeline, FXPipeline
from backend.data_pipeline.compiler import Compiler

class PipelineRunner:

    def __init__(self, price_pipeline : PricePipeline, action_pipeline : CorporateActionPipeline, fx_pipeline : FXPipeline,  base_save_path : Path):

        self.price_pipeline = price_pipeline
        self.action_pipeline = action_pipeline
        self.fx_pipeline = fx_pipeline
        self.base_save_path = Path(base_save_path)
        self.compiled_data = None
    

    def _save_csv_only(self, name: str, data: pl.DataFrame, stage: str) -> None: 
        """
        Save only a CSV file for inspection purposes.

        Args:
            name (str): Dataset name (e.g., 'prices').
            data (pl.DataFrame): DataFrame to save.
            stage (str): Stage of the pipeline ('cleaned', 'processed').
        """
        save_path = self.base_save_path / f"{stage}/csv/{name}.csv"
        save_csv(data, save_path)


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
        
        save_csv(data, csv_path)

        if partitioned:
            save_partitioned_parquet(data, partitioned_parquet_path)
        else:
            save_regular_parquet(data, regular_parquet_path)


    def run(self) -> None:
        """
        Run the entire master pipeline in sequence:
        - Ingest and process price and corporate action data.
        - Compile them into a single backtest-ready dataset.
        - Ingest and process FX data separately.
        - Save interim CSVs for human inspection.
        - Save final data (compiled prices and FX rates) in both CSV and Parquet formats.

        Raises:
            RuntimeError: If compilation of backtest data fails.
        """
        
        # --- PRICE PIPELINE ---
        try:
            self.price_pipeline.run()
            self._save_csv_only('prices',self.price_pipeline.cleaned_data,'cleaned')
            self._save_csv_only('prices',self.price_pipeline.processed_data,'processed')
        except Exception as e:
            warnings.warn(f"Error in price pipeline : {e}")

        # --- CORPORATE ACTION PIPELINE ---
        try:
            self.action_pipeline.run()
            self._save_csv_only('corporate_actions',self.action_pipeline.cleaned_data,'cleaned')
            self._save_csv_only('corporate_actions',self.action_pipeline.processed_data,'processed')
        except Exception as e:
            warnings.warn(f"Error in corporate action pipeline : {e}")
        
        # --- COMPILE TO FINAL BACKTEST DATASET ---
        try:
            print("Running backtest data compilation...")
            self.compiled_data = Compiler.compile(self.price_pipeline.processed_data, self.action_pipeline.processed_data)
            self._save_csv_and_parquet('data',self.compiled_data,'compiled',partitioned=True)
        except Exception as e:
            raise RuntimeError(f"Critical error when compiling backtest data.") from e
        
        # --- FX PIPELINE ---
        try:
            self.fx_pipeline.run()
            self._save_csv_only('fx',self.fx_pipeline.cleaned_data,'cleaned')
            self._save_csv_and_parquet('fx',self.fx_pipeline.processed_data,'processed',partitioned=False)
        except Exception as e:
            warnings.warn(f"Error in fx pipeline : {e}")
        
        print("Pipeline execution complete.")