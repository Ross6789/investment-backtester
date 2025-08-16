import polars as pl
import warnings
from pathlib import Path
import backend.core.paths as paths
from backend.utils.saving import save_csv, save_regular_parquet,save_partitioned_parquet
from backend.utils.timing import timing
from backend.utils.metadata import generate_asset_metadata_json, generate_benchmark_metadata_json, update_asset_metadata_csv, update_benchmark_metadata_csv

from backend.data_pipeline.pipelines import PricePipeline, CorporateActionPipeline, FXPipeline,BenchmarkPipeline, BasePipeline
from backend.data_pipeline.compiler import Compiler

class PipelineRunner:

    def __init__(self, price_pipeline : PricePipeline, action_pipeline : CorporateActionPipeline, benchmark_pipeline: BenchmarkPipeline, fx_pipeline : FXPipeline, dev_mode: bool = False):
        self.price_pipeline = price_pipeline
        self.action_pipeline = action_pipeline
        self.benchmark_pipeline = benchmark_pipeline
        self.fx_pipeline = fx_pipeline
        self.dev_mode = dev_mode
        self.base_save_path = paths.get_data_ingestion_path(dev_mode)

        # Default set of pipelines
        self.pipeline_names = {"price", "action", "benchmark", "fx"}
    
    

    # def _save_csv_only(self, name: str, data: pl.DataFrame, stage: str) -> None: 
    #     """
    #     Save only a CSV file for inspection purposes.

    #     Args:
    #         name (str): Dataset name (e.g., 'prices').
    #         data (pl.DataFrame): DataFrame to save.
    #         stage (str): Stage of the pipeline ('cleaned', 'processed').
    #     """
    #     save_path = self.base_save_path / f"{stage}/csv/{name}.csv"
    #     save_csv(data, save_path)


    # def _save_parquet_only(self, name: str, data: pl.DataFrame, stage: str, partitioned: bool = False) -> None:
    #     """
    #     Save only a Parquet format for final datasets.

    #     Args:
    #         name (str): Dataset name (e.g., 'data', 'fx').
    #         data (pl.DataFrame): DataFrame to save.
    #         stage (str): Stage of the pipeline ('processed', 'compiled').
    #         partitioned (bool): Whether to save as partitioned Parquet.
    #     """
    #     partitioned_parquet_path = self.base_save_path / f"{stage}/parquet/{name}" 
    #     regular_parquet_path = self.base_save_path / f"{stage}/parquet/{name}.parquet"
        
    #     if partitioned:
    #         save_partitioned_parquet(data, partitioned_parquet_path)
    #     else:
    #         save_regular_parquet(data, regular_parquet_path)



    # def _save_csv_and_parquet(self, name: str, data: pl.DataFrame, stage: str, partitioned: bool = False) -> None:
    #     """
    #     Save both CSV and Parquet formats for final datasets.

    #     Args:
    #         name (str): Dataset name (e.g., 'data', 'fx').
    #         data (pl.DataFrame): DataFrame to save.
    #         stage (str): Stage of the pipeline ('processed', 'compiled').
    #         partitioned (bool): Whether to save as partitioned Parquet.
    #     """
    #     csv_path = self.base_save_path / f"{stage}/csv/{name}.csv"
    #     partitioned_parquet_path = self.base_save_path / f"{stage}/parquet/{name}" 
    #     regular_parquet_path = self.base_save_path / f"{stage}/parquet/{name}.parquet"
        
    #     save_csv(data, csv_path)

    #     if partitioned:
    #         save_partitioned_parquet(data, partitioned_parquet_path)
    #     else:
    #         save_regular_parquet(data, regular_parquet_path)


    # def run_dev(self, pipelines_to_run: set[str]=None) -> None:
    #     """
    #     Run the entire master pipeline in sequence:
    #     - Ingest and process price and corporate action data.
    #     - Compile them into a single backtest-ready dataset.
    #     - Ingest and process FX data separately.
    #     - Ingest and process Benchmark data seperately
    #     - Save interim CSVs for human inspection.
    #     - Save final data (compiled historical prices, benchmarks and FX rates) in both CSV and Parquet formats.

    #     Raises:
    #         RuntimeError: If compilation of backtest data fails.
    #     """
    #     if pipelines_to_run is None:
    #         pipelines_to_run = self.pipeline_names
        
    #     timings = {}

    #     # --- PRICE PIPELINE ---
    #     if "price" in pipelines_to_run:
    #         try:
    #             with timing("Price pipeline",timings):
    #                 self.price_pipeline.run()
    #                 self._save_csv_only('prices',self.price_pipeline.cleaned_data,'cleaned')
    #                 self._save_csv_only('prices',self.price_pipeline.processed_data,'processed')
    #         except Exception as e:
    #             warnings.warn(f"Error in price pipeline : {e}")

    #     # --- CORPORATE ACTION PIPELINE ---
    #     if "action" in pipelines_to_run:
    #         try:
    #             with timing("Corporate action pipeline",timings):
    #                 self.action_pipeline.run()
    #                 self._save_csv_only('corporate_actions',self.action_pipeline.cleaned_data,'cleaned')
    #                 self._save_csv_only('corporate_actions',self.action_pipeline.processed_data,'processed')
    #         except Exception as e:
    #             warnings.warn(f"Error in corporate action pipeline : {e}")
        
    #     # --- COMPILE TO FINAL BACKTEST DATASET ---
    #     if "price" in pipelines_to_run and "action" in pipelines_to_run:
    #         try:
    #             print("Running backtest data compilation...")
    #             with timing("Data compilation",timings):
    #                 self.compiled_data = Compiler.compile(self.price_pipeline.processed_data, self.action_pipeline.processed_data)
    #                 self._save_csv_and_parquet('data',self.compiled_data,'compiled',partitioned=True)
    #         except Exception as e:
    #             raise RuntimeError(f"Critical error when compiling backtest data: {e}") from e
        
    #     # --- BENCHMARK PIPELINE (USES EXISTING PRICE PIPELINE CLASS BUT SEPERATE SAVE LOCATION)---
    #     if "benchmark" in pipelines_to_run:
    #         try:
    #             with timing("Benchmark pipeline",timings):
    #                 self.benchmark_pipeline.run()
    #                 self._save_csv_only('benchmarks',self.benchmark_pipeline.cleaned_data,'cleaned')
    #                 self._save_csv_and_parquet('benchmarks',self.benchmark_pipeline.processed_data,'processed',partitioned=False)
    #         except Exception as e:
    #             warnings.warn(f"Error in benchmark pipeline : {e}")
        
    #     # --- FX PIPELINE ---
    #     if "fx" in pipelines_to_run:
    #         try:
    #             with timing("FX pipeline",timings):
    #                 self.fx_pipeline.run()
    #                 self._save_csv_only('fx',self.fx_pipeline.cleaned_data,'cleaned')
    #                 self._save_csv_and_parquet('fx',self.fx_pipeline.processed_data,'processed',partitioned=False)
    #         except Exception as e:
    #             warnings.warn(f"Error in fx pipeline : {e}")
        
    #     print("Pipeline execution complete.")

    #     # Print timings
    #     print("\n--- Pipeline Timings ---")
    #     for stage, seconds in timings.items():
    #         print(f"{stage:<30} {seconds:.2f} seconds")
    #     print("\n")


    def _save_inspection_data(self,name:str,data:pl.DataFrame,stage:str):
            save_csv(data, self.base_save_path / f"inspection/{stage}/{name}.csv")



    def _save_operational_data(self,name:str,data:pl.DataFrame,*,partitioned: bool=False):
        path = self.base_save_path / name
        if partitioned:
            save_partitioned_parquet(data, path)
        else:
            save_regular_parquet(data, path.with_suffix(".parquet"))

    def run(self, *, pipelines_to_run: set[str] = None):
        """
        Options for pipelines_to_run : "price", "action", "benchmark", "fx"
        """
        if pipelines_to_run is None:
            pipelines_to_run = self.pipeline_names
        
        timings = {}

        # --- FX PIPELINE ---
        if "fx" in pipelines_to_run:
            try:
                with timing("FX pipeline",timings):
                    self.fx_pipeline.run()
                    if self.dev_mode:
                        self._save_inspection_data('fx-rates',self.fx_pipeline.cleaned_data,'cleaned')
                        self._save_inspection_data('fx-rates',self.fx_pipeline.processed_data,'processed')
                    self._save_operational_data('fx-rates',self.fx_pipeline.processed_data,partitioned=False)
            except Exception as e:
                warnings.warn(f"Error in fx pipeline : {e}")

        # --- PRICE PIPELINE ---
        if "price" in pipelines_to_run:
            try:
                with timing("Price pipeline",timings):
                    self.price_pipeline.run()
                    if self.dev_mode:
                        self._save_inspection_data('historical-prices',self.price_pipeline.cleaned_data,'cleaned')
                        self._save_inspection_data('historical-prices',self.price_pipeline.processed_data,'processed')
            except Exception as e:
                warnings.warn(f"Error in price pipeline : {e}")

        # --- CORPORATE ACTION PIPELINE ---
        if "action" in pipelines_to_run:
            try:
                with timing("Corporate action pipeline",timings):
                    self.action_pipeline.run()
                    if self.dev_mode: 
                        self._save_inspection_data('corporate-actions',self.action_pipeline.cleaned_data,'cleaned')
                        self._save_inspection_data('corporate-actions',self.action_pipeline.processed_data,'processed')
            except Exception as e:
                warnings.warn(f"Error in corporate action pipeline : {e}")
        
        # --- COMPILE TO FINAL BACKTEST DATASET ---
        if "price" in pipelines_to_run and "action" in pipelines_to_run:
            try:
                print("Running backtest data compilation...")
                with timing("Data compilation",timings):
                    self.compiled_data = Compiler.compile(self.price_pipeline.processed_data, self.action_pipeline.processed_data)
                    if self.dev_mode: 
                        self._save_inspection_data('historical-prices',self.compiled_data,'compiled')
                        self._save_operational_data('historical-prices',self.compiled_data, partitioned=True) # Dev data gets partitioned as it will be read locally from disk ie. should be partition for lazy operations
                    else :
                        self._save_operational_data('historical-prices',self.compiled_data, partitioned=False) # Production data does not get partitioned as it will be stored on cloud and will need to be accessed via api call (partition = throttling)
            except Exception as e:
                raise RuntimeError(f"Critical error when compiling backtest data: {e}") from e
        
        # --- BENCHMARK PIPELINE (USES EXISTING PRICE PIPELINE CLASS BUT SEPERATE SAVE LOCATION)---
        if "benchmark" in pipelines_to_run:
            try:
                with timing("Benchmark pipeline",timings):
                    self.benchmark_pipeline.run()
                    if self.dev_mode:
                        self._save_inspection_data('benchmarks',self.benchmark_pipeline.cleaned_data,'cleaned')
                        self._save_inspection_data('benchmarks',self.benchmark_pipeline.processed_data,'processed')
                    self._save_operational_data('benchmarks',self.benchmark_pipeline.processed_data,partitioned=False)
            except Exception as e:
                warnings.warn(f"Error in benchmark pipeline : {e}")

        # --- Update Metadata CSV based on new data ingested (active dates and whether a ticker pays dividends) ---
        if ("price" in pipelines_to_run and "action" in pipelines_to_run) or "benchmark" in pipelines_to_run:
            try:
                with timing("CSV Metadata update",timings):
                    if ("price" in pipelines_to_run and "action" in pipelines_to_run):
                        update_asset_metadata_csv(self.compiled_data)
                    if "benchmark" in pipelines_to_run:
                        update_benchmark_metadata_csv(self.benchmark_pipeline.processed_data)
            except Exception as e:
                warnings.warn(f"Error in CSV Metadata update: {e}")
                

        # --- Generate JSON for frontend ---
        if "price" in pipelines_to_run or "benchmark" in pipelines_to_run:
            try:
                with timing("JSON generation",timings):
                    if "price" in pipelines_to_run:
                        generate_asset_metadata_json(self.dev_mode)
                    if "benchmark" in pipelines_to_run:
                        generate_benchmark_metadata_json(self.dev_mode)
            except Exception as e:
                warnings.warn(f"Error in JSON generation: {e}")

        
        print("Pipeline execution complete.")

        # Print timings
        print("\n--- Pipeline Timings ---")
        for stage, seconds in timings.items():
            print(f"{stage:<30} {seconds:.2f} seconds")
        print("\n")

