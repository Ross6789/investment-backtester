import polars as pl
from backend.core.paths import get_production_benchmark_data_path
from backend.data_pipeline.processors import BenchmarkProcessor

benchmarks_path = get_production_benchmark_data_path()

benchmarks = pl.read_parquet(benchmarks_path)

converted = BenchmarkProcessor.convert_prices_to_all_base_currencies(benchmark_prices_df=benchmarks).collect()

print(converted)