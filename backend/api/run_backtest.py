import backend.core.paths as paths
import polars as pl
from pathlib import Path
from datetime import date
from backend.backtest.data_loader import fetch_filtered_backtest_data
from backend.core.models import TargetPortfolio, RecurringInvestment, BacktestConfig, Strategy
from backend.core.enums import BacktestMode, BaseCurrency, RebalanceFrequency, ReinvestmentFrequency
from backend.core.parsers import parse_enum, parse_date
from backend.backtest.runner import BacktestRunner



def async_run_backtest (jobs, job_id, input_data, dev_mode: bool = False):
    try:
        # Run your current backtest function
        backtest_result, temp_excel_path = run_backtest(input_data, dev_mode)

        # Store results in job
        download_link = f"/api/download-report?file={temp_excel_path}" if temp_excel_path else None
        jobs[job_id]["status"] = "done"
        jobs[job_id]["result"] = {"success": True, "excel_download": download_link, **backtest_result}

    except Exception as e:
        jobs[job_id]["status"] = "error"
        jobs[job_id]["result"] = {"success": False, "error": str(e)}
        raise Exception(e)
    

def run_backtest(input_data: dict, dev_mode: bool = False) -> dict:
   
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
    export_excel = input_data["export_excel"]
        
    # Create config objects
    target_portfolio = TargetPortfolio(target_weights)
    strategy = Strategy(fractional_shares,reinvest_dividends,rebalance_frequency)
    backtest_config = BacktestConfig(start_date,end_date,target_portfolio,mode,base_currency,strategy,initial_investment,recurring_investment)

    # Fetch and filter backtest data from cache 
    price_data, benchmark_data = fetch_filtered_backtest_data(mode,base_currency,target_portfolio.get_tickers(),start_date,end_date)


    # Create and run backtest
    backtest = BacktestRunner(backtest_config, price_data, benchmark_data, export_excel, dev_run=dev_mode, base_save_path=paths.get_backtest_run_base_path())
    results, temp_excel_path = backtest.run()

    return {
        "settings":input_data,
        "results":results,
    }, temp_excel_path

