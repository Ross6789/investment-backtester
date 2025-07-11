from unittest.mock import patch
import pytest
import polars as pl
from backend.data_pipeline.ingestors import CSVIngestor
from datetime import date
from polars.testing import assert_frame_equal as pl_assert_frame_equal

@pytest.fixture
def get_fixture_value(request):
    return request.getfixturevalue(request.param)

@pytest.fixture
def sample_2col_df_raw():
    return pl.DataFrame({
        'date':['01/01/2025','02/01/2025','03/01/2025','04/01/2025'],
        'close':[1000,1002,1004,1006]
    })

@pytest.fixture
def expected_transformed_2col_df_full():
    return pl.DataFrame({
        'date':[date(2025,1,1),date(2025,1,2),date(2025,1,3),date(2025,1,4)],
        'adj_close':[1000,1002,1004,1006],
        'close':[1000,1002,1004,1006],
        'ticker':['AAPL','AAPL','AAPL','AAPL']
    })

@pytest.fixture
def expected_transformed_2col_df_filtered():
    return pl.DataFrame({
        'date':[date(2025,1,2),date(2025,1,3)],
        'adj_close':[1002,1004],
        'close':[1002,1004],
        'ticker':['AAPL','AAPL']
    })

@pytest.fixture
def sample_3col_df_raw():
    return pl.DataFrame({
        'date':['01/01/2025','02/01/2025','03/01/2025','04/01/2025'],
        'adj_close':[1003,1005,1007,1009],
        'close':[1000,1002,1004,1006]
    })

@pytest.fixture
def expected_transformed_3col_df_full():
    return pl.DataFrame({
        'date':[date(2025,1,1),date(2025,1,2),date(2025,1,3),date(2025,1,4)],
        'adj_close':[1003,1005,1007,1009],
        'close':[1000,1002,1004,1006],
        'ticker':['AAPL','AAPL','AAPL','AAPL']
    })

@pytest.fixture
def expected_transformed_3col_df_filtered():
    return pl.DataFrame({
        'date':[date(2025,1,2),date(2025,1,3)],
        'adj_close':[1005,1007],
        'close':[1002,1004],
        'ticker':['AAPL','AAPL']
    })

@pytest.fixture
def sample_invalid_1col_df_raw():
    return pl.DataFrame({
        'close':[1000,1002,1004,1006]
    })

@pytest.fixture
def sample_invalid_4col_df_raw():
    return pl.DataFrame({
        'date':['01/01/2025','02/01/2025','03/01/2025','04/01/2025'],
        'adj_close':[1003,1005,1007,1009],
        'close':[1000,1002,1004,1006],
        'volume':[1000000,1000500,1000900,1001100]
    })

@pytest.fixture
def sample_ingestor_with_dates():
    return CSVIngestor('AAPL','dummy_source_path.csv','2025-01-02','2025-01-03')

@pytest.fixture
def sample_ingestor_without_dates():
    return CSVIngestor('AAPL','dummy_source_path.csv',None,None)


@patch('backend.pipelines.ingestors.pl.read_csv')
def test_read_data_valid(mock_read_csv,sample_2col_df_raw, sample_ingestor_without_dates):
    mock_read_csv.return_value = sample_2col_df_raw

    ingestor = sample_ingestor_without_dates

    result = ingestor.read_data()

    mock_read_csv.assert_called_once_with('dummy_source_path.csv')

    pl_assert_frame_equal(result, sample_2col_df_raw)

csv_df = get_fixture_value
ingestor = get_fixture_value

@pytest.mark.parametrize('csv_df, ingestor',[
    ('sample_invalid_1col_df_raw','sample_ingestor_without_dates'),
    ('sample_invalid_4col_df_raw','sample_ingestor_without_dates'),
], indirect=True)
def test_transform_data_invalid(csv_df, ingestor):
    with pytest.raises(ValueError, match="Invalid number of columns in CSV file"):
        ingestor.transform_data(csv_df)

transformed_df = get_fixture_value

@pytest.mark.parametrize('csv_df,transformed_df,ingestor',[
    ('sample_2col_df_raw','expected_transformed_2col_df_full','sample_ingestor_without_dates'),
    ('sample_2col_df_raw','expected_transformed_2col_df_filtered','sample_ingestor_with_dates'),
    ('sample_3col_df_raw','expected_transformed_3col_df_full','sample_ingestor_without_dates'),
    ('sample_3col_df_raw','expected_transformed_3col_df_filtered','sample_ingestor_with_dates')
], indirect=True)
def test_transform_data(csv_df,transformed_df,ingestor):
    ingestor.transform_data(csv_df)
    pl_assert_frame_equal(ingestor.data,transformed_df) 

def test_run(monkeypatch,sample_2col_df_raw,expected_transformed_2col_df_full,sample_ingestor_without_dates):

    monkeypatch.setattr(sample_ingestor_without_dates,"read_data",lambda: sample_2col_df_raw)
    monkeypatch.setattr(sample_ingestor_without_dates,"transform_data", lambda data: setattr(sample_ingestor_without_dates,"data",expected_transformed_2col_df_full))

    result = sample_ingestor_without_dates.run()
    pl_assert_frame_equal(result, expected_transformed_2col_df_full)