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
    """
    Returns a list of yFinance tickers for the given asset type.

    Args:
        asset_type (str): Type of asset to filter by (e.g., 'stock', 'crypto').

    Returns:
        list[str]: List of matching ticker symbols from yFinance.
    """
    metadata = (
        pl.scan_csv(config.get_asset_metadata_path())
        .filter((pl.col("source")=="yfinance") & (pl.col("asset_type")==asset_type))
        .select("ticker")
        .collect()
    )
    return metadata["ticker"].to_list()


def get_fx_csv_sources() -> list[Path]:
    """
    Returns a list of all csv source paths for fx data

    Returns:
        list[Path]: List of all csv sources paths within the fx metadata file.
    """
    sources = (
        pl.scan_csv(config.get_fx_metadata_path())
        .filter(pl.col("source")=="local_csv")
        .select("source_file_path")
        .collect()
        .to_series()
        .to_list()
    )
    return sources


def get_asset_csv_sources() -> list[Path]:
    """
    Returns a list of all csv source paths for asset data

    Returns:
        list[Path]: List of all csv sources paths within the metadata file.
    """
    sources = (
        pl.scan_csv(config.get_asset_metadata_path())
        .filter(pl.col("source")=="local_csv")
        .select("source_file_path")
        .collect()
        .to_series()
        .to_list()
    )
    return sources


def get_csv_ticker_source_map() -> dict[str, Path]:
    """
    Returns a mapping of tickers to their local CSV file paths.

    Returns:
        dict[str, Path]: Dictionary where keys are ticker symbols and values are local CSV file paths.
    """
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
    for member in enum_class:
        if member.value.lower() == input_str.lower():
            return member
    valid_values = [e.value for e in enum_class]
    raise ValueError(f"Invalid value for '{enum_class.__name__}': '{input_str}'. Must be one of {valid_values}")
    

def parse_date(date_str: str) -> date:
    """
    Parses a date string into a `datetime.date` object using known formats.

    Tries multiple common date formats (e.g., "DD/MM/YYYY", "MM/DD/YYYY", "YYYY-MM-DD")

    Args:
        date_str (str): The date string to parse.

    Returns:
        date: The parsed date as a `datetime.date` object.

    Raises:
        ValueError: If the date string does not match any known format.
    """
    known_formats = ["%d/%m/%Y", "%m/%d/%Y", "%Y-%m-%d"]  # Add more if needed
    for fmt in known_formats:
        try:
            return datetime.strptime(date_str.strip(), fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Unknown date format: '{date_str}'")


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


def validate_int(obj, name : str) -> None:
    """Validate that the given object is an integer.

    Args:
        obj: The object to validate.
        name: The name of the variable (used for error messages).

    Raises:
        TypeError: If the object is not an instance of int.
    """
    if not isinstance(obj, int):
        raise TypeError(f"{name} must be an integer. Currently {type(obj)} : {obj} ")
    

# --- Scheduling Utilities ---

def generate_recurring_dates(start_date: date, end_date: date, frequency: str) -> set[date]:
    """
    Generate a set of recurring dates between start_date and end_date (inclusive),
    spaced according to the specified frequency. The start_date itself is excluded
    from the returned set.

    Args:
        start_date (date): The starting date of the range (excluded from results).
        end_date (date): The ending date of the range (inclusive).
        frequency (str): Recurrence frequency. Valid values are
            'daily', 'weekly', 'monthly', 'quarterly', 'yearly'.

    Returns:
        set[date]: A set of dates recurring at the specified frequency within the
            date range, excluding the start_date.

    Raises:
        ValueError: If the provided frequency is not one of the valid options.
    """
    time_spacing_map = {
        'daily': relativedelta(days=1),
        'weekly': relativedelta(weeks=1),
        'monthly': relativedelta(months=1),
        'quarterly': relativedelta(months=3),
        'yearly': relativedelta(years=1),
    }

    if frequency not in time_spacing_map:
        raise ValueError(f'Invalid frequency: {frequency}')

    dates = set()
    time_spacing = time_spacing_map[frequency]
    current_date = start_date + time_spacing 

    while current_date <= end_date:
        dates.add(current_date)
        current_date += time_spacing

    return dates


# --- Saving Utilities ---

def save_partitioned_parquet(data : pl.DataFrame, directory_save_path: Path) -> None:
    """
    Save a Polars DataFrame as a partitioned Parquet dataset, grouped by ticker.

    Each group (by 'ticker') is saved in its own subdirectory, suitable for efficient querying.

    Args:
        data (pl.DataFrame): The DataFrame to save. Must contain a 'ticker' column.
        directory_save_path (Path): Root directory to save the partitioned dataset.

    Raises:
        RuntimeError: If any error occurs while writing the Parquet files.
    """
    try:
        for (ticker,) , ticker_df in data.group_by("ticker"):
            folder = directory_save_path / f"ticker={ticker}"
            folder.mkdir(parents=True, exist_ok=True)
            ticker_df.write_parquet(folder / "data.parquet")
        print(f"Data saved to {directory_save_path}.") 
    except Exception as e:
        raise RuntimeError(f"Failed to save partitioned parquet to {directory_save_path}: {e}") from e


def save_regular_parquet(data : pl.DataFrame, save_path: Path) -> None:
    """
    Save a Polars DataFrame as a single flat Parquet file.

    Args:
        data (pl.DataFrame): The DataFrame to save.
        save_path (Path): The full file path where the Parquet file should be saved.

    Raises:
        RuntimeError: If saving fails for any reason.
    """
    try:
        data.write_parquet(save_path)
        print(f"Data saved to {save_path}.") 
    except Exception as e:
        raise RuntimeError(f"Failed to save regular parquet to {save_path}: {e}") from e


def save_csv(data : pl.DataFrame, save_path: Path) -> None: 
    """
    Save a Polars DataFrame as a CSV file.

    Args:
        data (pl.DataFrame): The DataFrame to save.
        save_path (Path): The full file path where the CSV should be saved.

    Raises:
        RuntimeError: If writing the CSV file fails.
    """
    try:
        data.write_csv(save_path)
        print(f"Data saved to {save_path}.") 
    except Exception as e:
        raise RuntimeError(f"Failed to save CSV to {save_path}: {e}") from e
  

# --- Reporting Utilities --- 

def build_pivoted_col_names(pivot_values : list[str] , columns : list [str]) -> list[str]:
        """
        Generate new column names by combining each pivot value with each column name.

        Each new name is formatted as '<pivot_value>_<column>', effectively expanding 
        the original column names with a prefix for each pivot value.

        Args:
            pivot_values (list[str]): A list of pivot values (e.g. categories or keys).
            columns (list[str]): A list of column names to be prefixed.

        Returns:
            list[str]: A list of combined column names.
        """
        return [f'{col}_{pivot}' for pivot in pivot_values for col in columns]


def build_drop_col_list(excluded_cols : list[str], all_cols : list[str]) -> list[str]:
    """
    Generate a list of column names to drop by excluding specified columns.

    Args:
        excluded_cols (list[str]): Columns to keep (e.g. join keys).
        all_cols (list[str]): All column names from a DataFrame.

    Returns:
        list[str]: Columns to drop (i.e. those not in excluded_cols).
    """
    return [col for col in all_cols if col not in excluded_cols]