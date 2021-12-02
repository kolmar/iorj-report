import pathlib
from datetime import datetime, timedelta


def ensure_directory(path):
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)


DateFormat = '%Y-%m-%d'


def format_date(date):
    return date.strftime(DateFormat)


def parse_date(date):
    return datetime.strptime(date, DateFormat)


def split_period(start, end):
    parsed_start = parse_date(start)
    parsed_end = parse_date(end)
    delta = (parsed_end - parsed_start).days

    if delta <= 0:
        raise ValueError('start >= end')

    middle = timedelta(days=delta // 2)
    return (
        (start, format_date(parsed_start + middle)), 
        (format_date(parsed_start + middle + timedelta(days=1)), end))

