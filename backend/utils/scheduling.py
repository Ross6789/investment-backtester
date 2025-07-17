from datetime import date
from dateutil.relativedelta import relativedelta


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

