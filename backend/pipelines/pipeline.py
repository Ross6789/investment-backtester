import polars as pl
from pathlib import Path

class DataPipeline:
    def __init__(self, ingestors, base_save_path, save_file_name):
        self.ingestors = ingestors  
        self.base_save_path = base_save_path
        self.processed_data = None

    # Method to save the data as a partitioned parquet file
    def _save_partitioned_parquet(self, data : pl.DataFrame, directory_save_path: str):
        save_dir_base_path = Path(directory_save_path)
        for (ticker,) , ticker_df in data.group_by("ticker"):
            folder = save_dir_base_path / f"ticker={ticker}"
            folder.mkdir(parents=True, exist_ok=True)
            ticker_df.write_parquet(folder / "data.parquet")
        print(f"Data saved to {save_dir_base_path}.")  

    # Method to save the data as a human readable csv
    def _save_csv(self, data : pl.DataFrame, csv_save_path: str):
        data.write_csv(csv_save_path)
        print(f"Data saved to {csv_save_path}.") 

    def ingest(self):
        
        dfs = []
        for ingestor in self.ingestors:
            try:
                df = ingestor.run()
                if not df.is_empty():
                    dfs.append(df)
                else: 
                    print(f"Warning: no data returned from ingestor")
            except Exception as e:
                print(f"Error running ingestor : {e}")
                continue
        
        for df in dfs:
            print(df.head())
        
        if dfs:
            self.processed_data = pl.concat(dfs)
        else:
            raise ValueError("No data ingested, therefore file save has been aborted")
        
    def save(self):
        self._save_csv(self.processed_data, f'{self.base_save_path}/processed/csv/{self.save_file_name}.csv')
        self._save_partitioned_parquet(self.processed_data,f'{self.base_save_path}/processed/parquet/{self.save_file_name}')

    def run(self):
        self.ingest()
        self.save()
    
    
class MasterPipeline():
    def process(self, save_file_name):
        
        dfs = []
        for ingestor in self.ingestors:
            try:
                df = ingestor.run()
                if not df.is_empty():
                    dfs.append(df)
                else: 
                    print(f"Warning: no data returned from ingestor")
            except Exception as e:
                print(f"Error running ingestor : {e}")
                continue
        
        for df in dfs:
            print(df.head())
        
        if dfs:
            self.processed_data = pl.concat(dfs)
        else:
            raise ValueError("No data ingested, therefore file save has been aborted")

        self._save_csv(self.processed_data, f'{self.base_save_path}/processed/csv/{save_file_name}.csv')
        self._save_partitioned_parquet(self.processed_data,f'{self.base_save_path}/processed/parquet/{save_file_name}')
        


    def run(self):
        try:
            self.ingest()
        except Exception as e:
            raise Exception(f"data pipeline failed : {e}")
        