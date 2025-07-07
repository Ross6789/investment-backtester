import polars as pl
from backend import config
from datetime import datetime, date
from enum import Enum
from pathlib import Path
from dateutil.relativedelta import relativedelta
from math import floor, ceil
from backend.constants import FRACTIONAL_SHARE_PRECISION,PRICE_PRECISION, CURRENCY_PRECISION
from backend.enums import RoundMethod

# --- Metadata file utilities ---

def get_yfinance_tickers(asset_type: str) -> list[str]:
    metadata = (
        pl.scan_csv(config.get_asset_metadata_path())
        .filter((pl.col("source")=="yfinance") & (pl.col("asset_type")==asset_type))
        .select("ticker")
        .collect()
    )

    return metadata["ticker"].to_list()

def get_csv_ticker_source_map() -> dict[str, Path]:
    metadata = (
        pl.scan_csv(config.get_asset_metadata_path())
        .filter(pl.col("source")=="local_csv")
        .select("ticker","source_file_path")
        .collect()
    )
    return {ticker: Path(source_path) for ticker, source_path in metadata.select(["ticker","source_file_path"]).iter_rows()}

    
# --- Rounding Utilities ---

def _round(value: float, decimals: int, method : RoundMethod ) -> float:
    """
    Round a float value to a specified number of decimal places using a given rounding method.

    Args:
        value (float): The number to round.
        decimals (int): The number of decimal places to round to.
        method (RoundMethod): The rounding method to use. One of "nearest", "down", or "up".

    Returns:
        float: The rounded value.

    Raises:
        ValueError: If an invalid rounding method is provided.
    """
    factor =  10 ** decimals
    match method:
        case "nearest":
            return round(value,decimals)
        case "down":
            return floor(value * factor) / factor
        case "up":
            return ceil(value * factor) / factor
        case _:
            raise ValueError(f"Invalid rounding method : {method}")

def round_shares(share_qty: float, method: RoundMethod = "down") -> float:
    """
    Round the fractional share quantity to the configured fractional share precision.

    Args:
        share_qty (float): The quantity of shares to round.
        method (RoundMethod, optional): The rounding method to use. Defaults to "down".

    Returns:
        float: The rounded share quantity.
    """
    return _round(share_qty,FRACTIONAL_SHARE_PRECISION,method)

def round_price(price: float, method: RoundMethod = "nearest") -> float:
    """
    Round a price value to the configured price precision.

    Args:
        price (float): The price value to round.
        method (RoundMethod, optional): The rounding method to use. Defaults to "nearest".

    Returns:
        float: The rounded price.
    """
    return _round(price,PRICE_PRECISION,method)

def round_currency(price: float, method: RoundMethod = "nearest") -> float:
    """
    Round a currency amount to the configured currency precision.

    Args:
        price (float): The currency amount to round.
        method (RoundMethod, optional): The rounding method to use. Defaults to "nearest".

    Returns:
        float: The rounded currency amount.
    """
    return _round(price,CURRENCY_PRECISION,method)

# --- Parse Utilities ---

def parse_enum(enum_class: type[Enum], input_str: str) -> Enum:
    """
    Convert a string to an instance of the specified Enum class, case-insensitively.

    Args:
        enum_class (type[Enum]): The Enum class to parse the string into.
        input_str (str): The input string to convert to an Enum member.

    Returns:
        Enum: The corresponding Enum member matching the input string (case-insensitive).

    Raises:
        ValueError: If the input string does not match any Enum member values.
    """
    try:
        return enum_class(input_str.lower())
    except ValueError:
        valid_values = [e.value for e in enum_class]
        raise ValueError(
            f"Invalid value for '{enum_class.__name__}': '{input_str}'. Must be one of {valid_values} "
        )
    

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


# --- Validation Utilities ---

def validate_positive_amount(amount: float, field_name: str) -> None:
    """
    Validate that the given amount is positive.

    Args:
        amount (float): The numeric value to validate.
        field_name (str): The name of the field being validated, used in error messages.

    Raises:
        ValueError: If the amount is not greater than zero.
    """
    if amount <= 0:
        raise ValueError(f"Invalid {field_name}: '{amount}'. Must be positive.")
    

def validate_date_order(start_date: date, end_date: date) -> None:
    """
    Validate that the start_date is not after the end_date.

    Args:
        start_date (date): The start date.
        end_date (date): The end date.

    Raises:
        ValueError: If end_date is earlier than start_date.
    """
    if end_date < start_date :
        raise ValueError("Invalid dates : start date must be before end date")
    