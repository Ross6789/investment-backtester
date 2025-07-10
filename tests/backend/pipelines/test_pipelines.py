import pytest
from datetime import date
import polars as pl
from unittest.mock import MagicMock, patch
from backend.data_pipeline.pipeline import DataPipeline 
from polars.testing import assert_frame_equal

@pytest.fixture
def sample_pl_df_1():
    return pl.DataFrame({
        'date':[date(2025,1,1),date(2025,1,2),date(2025,1,1),date(2025,1,2)],
        'adj_close':[1003,1005,503,505],
        'close':[1000,1002,500,502],
        'ticker':['AAPL','AAPL','GOOG','GOOG']
    })

@pytest.fixture
def sample_pl_df_2():
    return pl.DataFrame({
        'date':[date(2025,1,1),date(2025,1,2),date(2025,1,1),date(2025,1,2)],
        'adj_close':[2006,2010,1006,1010],
        'close':[2000,2004,1000,1004],
        'ticker':['MSFT','MSFT','TSLA','TSLA']
    })

@pytest.fixture
def sample_pl_df_combined():
    return pl.DataFrame({
        'date':[date(2025,1,1),date(2025,1,2),date(2025,1,1),date(2025,1,2),date(2025,1,1),date(2025,1,2),date(2025,1,1),date(2025,1,2)],
        'adj_close':[1003,1005,503,505,2006,2010,1006,1010],
        'close':[1000,1002,500,502,2000,2004,1000,1004],
        'ticker':['AAPL','AAPL','GOOG','GOOG','MSFT','MSFT','TSLA','TSLA']
    })

@pytest.fixture
def mock_valid_ingestors(sample_pl_df_1,sample_pl_df_2):
    ingestor1 = MagicMock()
    ingestor1.run.return_value = sample_pl_df_1
    ingestor2 = MagicMock()
    ingestor2.run.return_value = sample_pl_df_2
    return [ingestor1, ingestor2]

@pytest.fixture
def mock_ingestor_fail():
    ingestor = MagicMock()
    ingestor.run.side_effect = RuntimeError('Simulated failure')
    return ingestor

@pytest.fixture
def pipeline_valid(mock_valid_ingestors):
    return DataPipeline(mock_valid_ingestors, 'dummy_save_path')

@pytest.fixture
def pipeline_invalid(mock_ingestor_fail):
    return DataPipeline(mock_ingestor_fail, 'dummy_save_path')

def test_combine_data_valid(pipeline_valid, sample_pl_df_combined):
    
    expected_df = sample_pl_df_combined
    pipeline_valid.combine_data()

    assert_frame_equal(pipeline_valid.combined_data, expected_df)

def test_combine_data_invalid(pipeline_invalid):
    with pytest.raises(ValueError, match='No data ingested, therefore file save has been aborted'):
        pipeline_invalid.combine_data()


def test_save_data(tmp_path, sample_pl_df_combined):
    save_dir = tmp_path / 'test_output'
    df = sample_pl_df_combined

    pipeline = DataPipeline(['dummy_ingestors'], save_dir)
    pipeline.combined_data = df 

    pipeline.save_data()

    tickers = sample_pl_df_combined.select('ticker').unique().to_series().to_list()

    for ticker in tickers:
        partitioned_dir = save_dir / f'ticker={ticker}'
        assert partitioned_dir.exists()
        assert any(partitioned_dir.glob('*parquet'))

    # Check partitioned directory exists, the parquet files will be stored within these folders
    assert(save_dir / 'ticker=AAPL').exists()
    assert(save_dir / 'ticker=GOOG').exists()

def test_run(monkeypatch, pipeline_valid):

    monkeypatch.setattr(pipeline_valid, "combine_data", MagicMock())
    monkeypatch.setattr(pipeline_valid, "save_data", MagicMock())

    pipeline_valid.run()

    pipeline_valid.combine_data.assert_called_once()
    pipeline_valid.save_data.assert_called_once()