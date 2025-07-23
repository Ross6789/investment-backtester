
def generate_suffixed_col_names(columns: list[str], suffixes: list[str]) -> list[str]:
    """
    Generate new column names by combining each column name with each suffix value.

    Each new name is formatted as '<column>_<suffix>', effectively expanding
    the original column names with each suffix.

    Args:
        columns (list[str]): A list of base column names.
        suffixes (list[str]): A list of suffix values (e.g., categories or keys).

    Returns:
        list[str]: A list of combined column names.
    """
    return [f'{col}_{suffix}' for suffix in suffixes for col in columns]


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

