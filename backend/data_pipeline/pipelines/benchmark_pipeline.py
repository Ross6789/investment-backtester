from backend.data_pipeline.pipelines import PricePipeline
from backend.data_pipeline.processors import BenchmarkProcessor, PriceProcessor



class BenchmarkPipeline(PricePipeline):
    """
    Extends PricePipeline to include currency conversion of benchmark prices into all backtest base currencies after the normal processing steps.
    """

    def process(self) -> None:
        print("Running benchmark price processing...")
        self.processed_data = PriceProcessor.forward_fill(self.cleaned_data,add_trading_day=False)
        self.processed_data = BenchmarkProcessor.convert_prices_to_all_base_currencies(self.processed_data)

