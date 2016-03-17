import datetime


class ToolError(Exception):
    pass


def get_date_ranges(from_date, to_date, interval_in_years):
    """
    Retrieve pairs of date ranges based on interval number
    """
    if from_date and to_date and from_date > to_date:
        raise ToolError('to_date must be after from_date')
    if not interval_in_years:
        return [(from_date, to_date)]
    date_ranges = []
    date_b = from_date - datetime.timedelta(days=1)
    while date_b < to_date:
        date_a = date_b + datetime.timedelta(days=1)
        date_b = datetime.datetime(
            date_a.year + interval_in_years, date_a.month, date_a.day,
        ) - datetime.timedelta(days=1)
        if date_b > to_date:
            date_b = to_date
        date_ranges.append((date_a, date_b))
    return date_ranges


def load_unique_lines(source_path):
    if not source_path:
        return []
    source_text = open(source_path, 'rt').read().strip()
    lines = set((normalize_line(x) for x in source_text.splitlines()))
    return sorted(filter(lambda x: x, lines))


def normalize_line(x):
    x = x.replace(',', '')
    x = x.replace(';', '')
    return x.strip()
