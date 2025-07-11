import polars as pl
import warnings
from backend.data_pipeline.ingestors import CSVIngestor
from backend.data_pipeline.pipelines import BasePipeline
from backend.data_pipeline.processors import FXProcessor


class FXPipeline(BasePipeline):
    """
    A data pipeline for ingesting and processing foreign exchange (FX) rate data.

    Supports ingestion from CSV files named in the format 'FROM-TO.csv' (e.g., 'eur-gbp.csv'),
    cleans the data into a standard format, and generates inverse exchange rates.
    """

    def ingest(self):
        """
        Run all ingestors and clean the raw FX data into a unified format.

        CSV filenames should follow the naming pattern 'FROM-TO.csv' to indicate currency pairs.

        Raises:
            ValueError: If no data is successfully ingested from any source.
        """
        cleaned_dfs = []
        for ingestor in self.ingestors:
            try:
                raw_data = ingestor.run()
                if isinstance(ingestor, CSVIngestor):
                    from_currency,to_currency = ingestor.source_path.stem.split('-')  # ticker is retrieved from the file name without extention ie. eur-gbp.csv , from = eur, to = gbp 
                    cleaned_data = FXProcessor.clean_csv_data(raw_data, from_currency, to_currency)
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
        Forward fill rates for all dates, generate inverse FX rates and store the result in `self.processed_data`.
        """
        filled_data = FXProcessor.forward_fill(self.cleaned_data)
        filled_and_inverse_data = FXProcessor.generate_inverse_rates(filled_data)
        self.processed_data = filled_and_inverse_data


    def run(self) -> None:
        """
        Ingest and process the fx data in sequence.
        """
        self.ingest()
        self.process()
