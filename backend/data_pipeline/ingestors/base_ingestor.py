from abc import ABC, abstractmethod
import pandas as pd

class BaseIngestor(ABC):

    @abstractmethod
    def run(self) -> pd.DataFrame:
        pass