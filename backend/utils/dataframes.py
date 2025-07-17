import polars as pl
from backend.core.models import RoundingConfig


def round_dataframe_columns(df: pl.DataFrame, rounding_config: RoundingConfig | None = None) -> pl.DataFrame:
    """
    Round float columns in the DataFrame based on their semantic role.

    - Price columns (e.g., prices, costs, exchange rates) are rounded using the price precision.
    - Value, dividend, cash, and return-related columns are rounded using the currency precision.
    - Percentage columns (identified by 'percentage' in the name) are rounded using the percentage precision.
    - Other float columns are rounded to the general precision.
    - Non-float columns are left unchanged.

    Args:
        df (pl.DataFrame): Input DataFrame to round.
        rounding_config (RoundingConfig | None): Optional configuration object specifying precision for each type of column. If None, defaults from RoundingConfig() are used.

    Returns:
        pl.DataFrame: DataFrame with rounded numeric columns.
    """
    # Use default rounding config if not passed in
    if rounding_config is None:
        rounding_config = RoundingConfig()

    rounded_cols = []
    for col, dtype in df.schema.items():
        if not dtype.is_float():
            continue
        # Identify columns by keywords
        elif any(keyword in col.lower() for keyword in ['price','cost','exchange_rate']):
            rounded_cols.append(pl.col(col).round(rounding_config.price_precision).alias(col))
        elif any(keyword in col.lower() for keyword in ['value','dividend','cash','return','gain']):
            rounded_cols.append(pl.col(col).round(rounding_config.currency_precision).alias(col))
        elif any(keyword in col.lower() for keyword in ['percentage']):
            rounded_cols.append(pl.col(col).round(rounding_config.percentage_precision).alias(col))
        else:
            rounded_cols.append(pl.col(col).round(rounding_config.general_precision).alias(col))

    return df.with_columns(rounded_cols)


def convert_columns_to_percentage(df: pl.DataFrame, cols : list[str]) -> pl.DataFrame:
    """
    Convert one or more decimal-based columns (e.g. returns) to percentage format for reporting.

    This multiplies each column by 100 and replaces it with a new column named '{col}_percentage'.

    Args:
        df (pl.DataFrame): Input DataFrame.
        cols (list[str]): List of column names to convert.

    Returns:
        pl.DataFrame: DataFrame with percentage-formatted columns replacing the originals.
    """
    percentage_exprs = [
        (pl.col(col) * 100).alias(f"{col}_percentage") for col in cols
    ]
    return df.with_columns(percentage_exprs).drop(cols)


def flatten_dataframe_columns(df: pl.DataFrame) -> pl.DataFrame:
    """
    Flattens a Polars DataFrame for CSV export:
    - Unnests Struct columns.
    - Converts List columns to comma-separated strings.
    """
    for col, dtype in df.schema.items():
        if isinstance(dtype, pl.Struct):
            df = df.unnest(col)
        elif isinstance(dtype, pl.List):
            df = df.with_columns(
                pl.col(col).map_elements(
                    lambda x: ", ".join(map(str, x)) if x is not None else "",
                    return_dtype=pl.String
                ).alias(col)
            )
    return df


def stringify_list_columns(df: pl.DataFrame) -> pl.DataFrame:
    """
    Converts all list-type columns in a Polars DataFrame to comma-separated strings. (required for exporting to CSV)

    Args:
        df (pl.DataFrame): The Polars DataFrame containing potentially nested list columns.

    Returns:
        pl.DataFrame: A new DataFrame where all list-type columns have been converted to string representations.
    """
    new_cols = []
    for col, dtype in df.schema.items():
        if isinstance(dtype, pl.List):
            # Convert List(String) to a comma-separated string
            new_col = df[col].arr.join(separator=",").alias(col)
            new_cols.append(new_col)
        else:
            new_cols.append(df[col])
    return pl.DataFrame(new_cols)
