import polars as pl
from backend import config
from datetime import datetime, date
from typing import List
from dateutil.relativedelta import relativedelta

def get_yfinance_tickers(asset_type: str) -> list[str]:
    metadata = (
        pl.scan_csv(config.get_asset_metadata_path())
        .filter((pl.col("source")=="yfinance") & (pl.col("asset_type")==asset_type))
        .select("ticker")
        .collect()
    )
    return metadata["ticker"].to_list()

def get_csv_ticker_source_map() -> dict[str: str]:
    metadata = (
        pl.scan_csv(config.get_asset_metadata_path())
        .filter(pl.col("source")=="local_csv")
        .select("ticker","source_file_path")
        .collect()
    )
    return [{"ticker": ticker, "source_path": source_path} for ticker, source_path in metadata.select(["ticker","source_file_path"]).iter_rows()]

def parse_date(date_str: str) -> date:
    """
    Converts a date string in 'YYYY-MM-DD' format to a datetime.date object.

    Args:
        date_str (str): Date string, e.g. '2024-01-01'

    Returns:
        datetime.date: Parsed date object

    Raises:
        ValueError: If the input string is not in the expected format.
    """
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError as e:
        raise ValueError(f"Invalid date format: '{date_str}'. Expected 'YYYY-MM-DD'.") from e
    
# --- Scheduling Utilities ---

def get_scheduling_dates(start_date: date, end_date: date, frequency: str,trading_dates: List[date]) -> List[date]:
        
    """
    Generate a list of rebalance dates between a start and end date based on the strategy's rebalance frequency.

    Ensures all rebalance dates fall on actual trading days by finding the first trading day 
    on or after each target rebalance date.

    Args:
        start_date (date): The start date of the backtest period.
        end_date (date): The end date of the backtest period.
        frequency (str): The scheduling frequency e.g., 'daily', 'weekly', 'monthly', 'quarterly', 'yearly'.
        trading_dates (List[date]): A list of all valid trading dates.

    Returns:
        List[date]:A list of dates aligned to trading days and seperated by the scheduling frequency period.

    Raises:
        ValueError: If an invalid scheduling frequency is provided.
    """
    schedule_dates = []
        
    # Offset start date by frequency chosen
    match frequency.lower():
        case "never":
            return []
        case "daily":
            target_date = start_date + relativedelta(days=1)
        case "weekly":
            target_date = start_date + relativedelta(days=7)
        case "monthly":
            target_date = start_date + relativedelta(months=1)
        case "quarterly":
            target_date = start_date + relativedelta(months=3)
        case "yearly":
            target_date = start_date + relativedelta(years=1)
        case _: 
            raise ValueError(f"Invalid scheduling frequency : {frequency}")
        
    while target_date <= end_date:
        # Get the first trading date on or after the current target_date
        schedule_date = next((td for td in trading_dates if td >= target_date),None)
        
        if schedule_date is None or schedule_date > end_date:
            break
        schedule_dates.append(schedule_date)
            
        # Determine next target date based on frequency
        match frequency.lower():
            case "never":
                break
            case "daily":
                target_date += relativedelta(days=1)
            case "weekly":
                target_date += relativedelta(days=7)
            case "monthly":
                target_date += relativedelta(months=1)
            case "quarterly":
                target_date += relativedelta(months=3)
            case "yearly":
                target_date += relativedelta(years=1)
            case _: 
                raise ValueError(f"Invalid scheduling frequency : {frequency}")

    return schedule_dates