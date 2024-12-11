from importlib import metadata

from .calendar_registry import get_calendar, get_calendar_names
from .calendar_utils import convert_freq, date_range, merge_schedules

# TODO: is the below needed? Can I replace all the imports on the calendars with ".tradesignals_market_calendar"
from .ts_market_calendar import MarketCalendar, get_calendar, get_calendar_names, merge_schedules, date_range, convert_freq

# if running in development there may not be a package
try:
    __version__ = metadata.version("tradesignals-market-calendar")
except metadata.PackageNotFoundError:
    __version__ = "development"

__all__ = [
    "MarketCalendar",
    "get_calendar",
    "get_calendar_names",
    "merge_schedules",
    "date_range",
    "convert_freq",
]
