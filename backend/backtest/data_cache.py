import io
import requests
import polars as pl
import backend.core.paths as paths
from pathlib import Path
from functools import lru_cache
from typing import Union

# ---- Helper to fetch parquet from URL or path into Polars DataFrame ----
def _fetch_parquet(source: Union[str, Path]) -> pl.DataFrame:
    if isinstance(source, Path):
        # Local file/folder (partitioned)
        return pl.scan_parquet(source)
    else:
        # URL
        resp = requests.get(source)
        resp.raise_for_status()
        return pl.scan_parquet(io.BytesIO(resp.content))
    
# ---- LRU cache wrappers ----
@lru_cache(maxsize=1)
def get_cached_historical_prices(dev_mode: bool = False) -> pl.DataFrame:
    source = paths.get_historical_prices_data_source(dev_mode)
    print(f"price data : {source}")
    return _fetch_parquet(source)

@lru_cache(maxsize=1)
def get_cached_benchmarks(dev_mode: bool = False) -> pl.DataFrame:
    source = paths.get_benchmark_data_source(dev_mode)
    print(f"benchmark data : {source}")
    return _fetch_parquet(source)


@lru_cache(maxsize=1)
def get_cached_fx(dev_mode: bool = False) -> pl.DataFrame:
    source = paths.get_fx_data_source(dev_mode)
    print(f"fx data : {source}")
    return _fetch_parquet(source)

@lru_cache(maxsize=1)
def get_cached_asset_metadata() -> pl.DataFrame:
    path = paths.get_asset_metadata_csv_path()
    print(f"asset metadata : {path}")
    return pl.scan_csv(path)

@lru_cache(maxsize=1)
def get_cached_benchmarks_metadata() -> pl.DataFrame:
    path = paths.get_benchmark_metadata_csv_path()
    print(f"benchmark metadata : {path}")
    return pl.scan_csv(path)

@lru_cache(maxsize=1)
def get_cached_fx_metadata() -> pl.DataFrame:
    path = paths.get_fx_metadata_csv_path()
    print(f"fx metadata : {path}")
    return pl.scan_csv(path)


# ---- Preload all caches at server start ----
def preload_all_data(dev_mode: bool = False):
    mode = "development" if dev_mode else "production"
    print(f"Preloading all datasets from {mode} folder into memory...")
    get_cached_historical_prices(dev_mode)
    get_cached_benchmarks(dev_mode)
    get_cached_fx(dev_mode)
    get_cached_asset_metadata()
    get_cached_benchmarks_metadata()
    get_cached_fx_metadata()
    print("Preload complete.")