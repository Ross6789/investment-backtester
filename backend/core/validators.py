import polars as pl
from datetime import date
from backend.core.models import BaseCurrency
from backend.core.constants import CURRENCY_START_DATES


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


def validate_int(obj, name : str) -> None:
    """
    Validate that the given object is an integer.

    Args:
        obj: The object to validate.
        name: The name of the variable (used for error messages).

    Raises:
        TypeError: If the object is not an instance of int.
    """
    if not isinstance(obj, int):
        raise TypeError(f"{name} must be an integer. Currently {type(obj)} : {obj} ")
    

def validate_flat_dataframe(df: pl.DataFrame):
    """
    Validate that the DataFrame contains only flat (non-nested) columns.

    Args:
        df (pl.DataFrame): The DataFrame to validate.

    Raises:
        ValueError: If any column is of type List, Struct, or Object.
    """
    for col, dtype in df.schema.items():
        if dtype.base_type() in [pl.List, pl.Struct, pl.Object]:
            raise ValueError(f"Non-flat column detected: '{col}' has type {dtype}")
        
        
def validate_currency_active(currency : BaseCurrency, start_date: date):
    """
    Validate that the given currency was active on or after its allowed start date.

    Args:
        currency (BaseCurrency): The currency to validate.
        start_date (date): The start date to check.

    Raises:
        ValueError: If the start date is earlier than the currency's allowed start date.
    """
    min_date = CURRENCY_START_DATES.get(currency.value)
    if min_date and start_date < min_date:
        raise ValueError(f"Start date for {currency.value} cannot be before {min_date.isoformat()}")
    
