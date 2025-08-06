import backend.core.paths as paths
from backend.backtest.data_loader import get_backtest_data
from backend.core.models import TargetPortfolio, RecurringInvestment, BacktestConfig, Strategy
from backend.core.enums import BacktestMode, BaseCurrency, RebalanceFrequency, ReinvestmentFrequency
from backend.core.parsers import parse_enum, parse_date
from backend.backtest.runner import BacktestRunner


def run_backtest(input_data: dict) -> dict:
   
    # Parse input
    mode = parse_enum(BacktestMode, input_data["mode"])
    base_currency = parse_enum(BaseCurrency, input_data["base_currency"])
    start_date = parse_date(input_data["start_date"])
    end_date = parse_date(input_data["end_date"])
    target_weights = input_data["target_weights"]
    initial_investment = input_data["initial_investment"]

    strategy_data = input_data["strategy"]
    fractional_shares=strategy_data["fractional_shares"]
    reinvest_dividends=strategy_data["reinvest_dividends"]
    rebalance_frequency=parse_enum(RebalanceFrequency, strategy_data["rebalance_frequency"])

    recurring_data = input_data["recurring_investment"]
    if recurring_data is not None:
        recurring_investment_amount=recurring_data["amount"]
        recurring_investment_frequency = parse_enum(ReinvestmentFrequency, recurring_data["frequency"])
        recurring_investment = RecurringInvestment(recurring_investment_amount,recurring_investment_frequency)
    else:
        recurring_investment = None
        
    # Create config objects
    target_portfolio = TargetPortfolio(target_weights)
    strategy = Strategy(fractional_shares,reinvest_dividends,rebalance_frequency)
    backtest_config = BacktestConfig(start_date,end_date,target_portfolio,mode,base_currency,strategy,initial_investment,recurring_investment)

    # Fetch data for backtest
    backtest_data = get_backtest_data(mode,base_currency,target_portfolio.get_tickers(),start_date,end_date)

    # Create and run backtest
    backtest = BacktestRunner(backtest_config, backtest_data,paths.get_backtest_run_base_path())
    results = backtest.run()

    return results

