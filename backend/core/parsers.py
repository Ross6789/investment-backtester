from datetime import datetime, date
from enum import Enum

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
