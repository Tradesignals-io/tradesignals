"""
Common module for the tradesignals project.
"""

from .kafka import AvroProducer
from .market_calendar import (
    TradingDay,
    calc_trading_days_between,
    calc_trading_days_until,
    calc_trading_minutes_between,
    calc_trading_minutes_until,
    get_is_market_open,
    get_last_session_close,
    get_last_session_date,
    get_last_session_open,
    get_next_session_close,
    get_next_session_date,
    get_next_session_open,
    is_regular_trading_hours,
)
from .validators import (
    validate_enum,
    validate_enum_or_none,
    validate_file_write_path,
    validate_path,
    validate_str_not_empty,
)

__all__ = [
    "AvroProducer",
    "TradingDay",
    "get_last_session_date",
    "get_next_session_date",
    "get_is_market_open",
    "get_last_session_open",
    "get_last_session_close",
    "get_next_session_open",
    "get_next_session_close",
    "calc_trading_minutes_between",
    "calc_trading_days_between",
    "calc_trading_minutes_until",
    "calc_trading_days_until",
    "is_regular_trading_hours",
    "validate_path",
    "validate_file_write_path",
    "validate_enum",
    "validate_enum_or_none",
    "validate_str_not_empty",
]
