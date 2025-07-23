import pytest
import polars as pl
from datetime import date

from backend.backtest.engines import BasicEngine  
from backend.core.models import BacktestResult,BacktestConfig, TargetPortfolio
from backend.utils.scheduling import generate_recurring_dates


# --- Fixtures ---

@pytest.fixture
def mock_config():

    return BacktestConfig(
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 10),
        target_portfolio=TargetPortfolio({"AAPL": 0.5, "MSFT": 0.5}),
    )

@pytest.fixture
def mock_backtest_data():

    return pl.DataFrame({
        "date": [date(2025, 1, d) for d in range(1, 6)],
        "ticker": ["AAPL"] * 5,
        "base_price": [100.0 + d for d in range(5)],
        "is_trading_day":[True, True, False, False, True]
    })

@pytest.fixture
def basic_engine(mock_config, mock_backtest_data):
    return BasicEngine(config=mock_config, backtest_data=mock_backtest_data)


# --- TESTS ---

def test_init_sets_portfolio_and_rebalance_dates(basic_engine):
    assert basic_engine.portfolio is not None
    assert isinstance(basic_engine.rebalance_dates, set)


def test_rebalance(basic_engine):
    prices = {"AAPL": 100.0, "MSFT": 50.0}
    weights = {"AAPL": 0.5, "MSFT": 0.5}

    # Add unbalanced holdings 
    basic_engine.portfolio.holdings = {"AAPL": 10, "MSFT": 40}

    basic_engine.rebalance(current_date=date(2025, 1, 2), prices=prices, normalized_target_weights=weights)

    # Check that funds were invested according to weights
    invested_tickers = basic_engine.portfolio.holdings.keys()
    assert basic_engine.portfolio.holdings == {"AAPL": 15.0, "MSFT": 30.0}
    assert basic_engine.portfolio.did_rebalance is True


def test_run_result_structure(basic_engine):
    result = basic_engine.run()
    assert isinstance(result, BacktestResult)

    # Check dataframes in result
    assert isinstance(result.data, pl.DataFrame)
    assert isinstance(result.calendar, pl.DataFrame)
    assert isinstance(result.cash, pl.DataFrame)
    assert isinstance(result.holdings, pl.DataFrame)

    # Check dataframes are not empty
    assert len(result.data) > 0
    assert len(result.calendar) > 0
    assert len(result.cash) > 0
    assert len(result.holdings) > 0


