from abc import ABC, abstractmethod
from backend.data_pipeline.ingestors import BaseIngestor

class BasePipeline(ABC):
    def __init__(self, ingestors : list[BaseIngestor]):
        self.ingestors = ingestors  
        self.cleaned_data = None
        self.processed_data = None


    @abstractmethod
    def ingest(self) -> None:
        pass


    @abstractmethod
    def process(self) -> None:
        pass


    @abstractmethod
    def run(self) -> None:
        pass