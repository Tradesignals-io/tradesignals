"""
Market calendar utility functions.
"""

from datetime import date, datetime, timedelta, timezone
from typing import Union

from pandas_market_calendars import get_calendar


class TradingDay:
    """Class to get market calendar information."""

    def __init__(self, dt: Union[datetime, None] = None, market_code: str = 'XNYS'):
        """Initialize a TradingDay object.

        Args:
            dt : datetime, optional
                The current datetime (default is now in UTC).
            market_code : str, optional
                The market code (default is 'XNYS').

        Examples:
            >>> td = TradingDay()
            >>> td.open_hour
            9
            >>> td.close_hour
            16
        """
        if dt is None:
            dt = datetime.now(timezone.utc)
        self.market_code = market_code
        self.dt = dt
        self.nyse = get_calendar(market_code)
        self.schedule = self.nyse.schedule(start_date=dt - timedelta(days=10), end_date=dt + timedelta(days=10))
        self.current_session = self.schedule[self.schedule.index.date == dt.date()]
        self.open_hour = self.current_session.iloc[0].market_open.hour
        self.open_minute = self.current_session.iloc[0].market_open.minute
        self.close_hour = self.current_session.iloc[0].market_close.hour
        self.close_minute = self.current_session.iloc[0].market_close.minute
        self.tz = self.current_session.iloc[0].market_open.tzinfo
        self.hours_in_trading_day = (self.current_session.iloc[0].market_close - self.current_session.iloc[0].market_open).seconds // 3600
        self.minutes_in_trading_day = (self.current_session.iloc[0].market_close - self.current_session.iloc[0].market_open).seconds // 60
        self.is_market_open = self.nyse.is_session(dt)
        self.minutes_until_open = (self.current_session.iloc[0].market_open - dt).seconds // 60 if dt < self.current_session.iloc[0].market_open else 0
        self.minutes_until_close = (self.current_session.iloc[0].market_close - dt).seconds // 60 if dt < self.current_session.iloc[0].market_close else 0
        self.days_until_open = (self.current_session.iloc[0].market_open.date() - dt.date()).days if dt < self.current_session.iloc[0].market_open else 0
        self.session_type = "rth"  # Assuming regular trading hours for simplicity


class MarketCalendar:
    """Market calendar utility functions."""

    @staticmethod
    def get_last_session_open(dt: Union[datetime, None] = None, market_code: str = 'XNYS') -> datetime:
        """Get the last session's open datetime.

        Args:
            dt : datetime, optional
                The current datetime (default is now in UTC).
            market_code : str, optional
                The market code (default is 'XNYS').

        Returns:
            datetime
                The last session's open datetime.

        Examples:
            >>> MarketCalendar.get_last_session_open()
            Timestamp('2023-03-10 14:30:00+0000', tz='UTC')
        """
        if dt is None:
            dt = datetime.now(timezone.utc)
        nyse = get_calendar(market_code)
        schedule = nyse.schedule(start_date=dt - timedelta(days=10), end_date=dt)
        last_session = schedule[schedule.index < dt].iloc[-1].market_open
        return last_session.tz_convert(timezone.utc)

    @staticmethod
    def get_last_session_close(dt: Union[datetime, None] = None, market_code: str = 'XNYS') -> datetime:
        """Get the last session's close datetime.

        Args:
            dt : datetime, optional
                The current datetime (default is now in UTC).
            market_code : str, optional
                The market code (default is 'XNYS').

        Returns:
            datetime
                The last session's close datetime.

        Examples:
            >>> MarketCalendar.get_last_session_close()
            Timestamp('2023-03-10 21:00:00+0000', tz='UTC')
        """
        if dt is None:
            dt = datetime.now(timezone.utc)
        nyse = get_calendar(market_code)
        schedule = nyse.schedule(start_date=dt - timedelta(days=10), end_date=dt)
        last_session = schedule[schedule.index < dt].iloc[-1].market_close
        return last_session.tz_convert(timezone.utc)

    @staticmethod
    def get_next_session_open(dt: Union[datetime, None] = None, market_code: str = 'XNYS') -> datetime:
        """Get the next session's open datetime.

        Args:
            dt : datetime, optional
                The current datetime (default is now in UTC).
            market_code : str, optional
                The market code (default is 'XNYS').

        Returns:
            datetime
                The next session's open datetime.

        Examples:
            >>> MarketCalendar.get_next_session_open()
            Timestamp('2023-03-13 14:30:00+0000', tz='UTC')
        """
        if dt is None:
            dt = datetime.now(timezone.utc)
        nyse = get_calendar(market_code)
        schedule = nyse.schedule(start_date=dt, end_date=dt + timedelta(days=10))
        next_session = schedule[schedule.index > dt].iloc[0].market_open
        return next_session.tz_convert(timezone.utc)

    @staticmethod
    def get_next_session_close(dt: Union[datetime, None] = None, market_code: str = 'XNYS') -> datetime:
        """Get the next session's close datetime.

        Args:
            dt : datetime, optional
                The current datetime (default is now in UTC).
            market_code : str, optional
                The market code (default is 'XNYS').

        Returns:
            datetime
                The next session's close datetime.

        Examples:
            >>> MarketCalendar.get_next_session_close()
            Timestamp('2023-03-13 21:00:00+0000', tz='UTC')
        """
        if dt is None:
            dt = datetime.now(timezone.utc)
        nyse = get_calendar(market_code)
        schedule = nyse.schedule(start_date=dt, end_date=dt + timedelta(days=10))
        next_session = schedule[schedule.index > dt].iloc[0].market_close
        return next_session.tz_convert(timezone.utc)

    @staticmethod
    def get_is_market_open(dt: Union[datetime, None] = None, market_code: str = 'XNYS') -> bool:
        """Check if the market is open.

        Args:
            dt : datetime, optional
                The current datetime (default is now in UTC).
            market_code : str, optional
                The market code (default is 'XNYS').

        Returns:
            bool
                True if the market is open, False otherwise.

        Examples:
            >>> MarketCalendar.get_is_market_open()
            False
        """
        if dt is None:
            dt = datetime.now(timezone.utc)
        nyse = get_calendar(market_code)
        return nyse.is_session(dt)

    @staticmethod
    def get_minutes_until_market_open(dt: Union[datetime, None] = None, market_code: str = 'XNYS') -> int:
        """Calculate the minutes until the market is open.

        Args:
            dt : datetime, optional
                The current datetime (default is now in UTC).
            market_code : str, optional
                The market code (default is 'XNYS').

        Returns:
            int
                The minutes until the market is open.

        Examples:
            >>> MarketCalendar.get_minutes_until_market_open()
            120
        """
        if dt is None:
            dt = datetime.now(timezone.utc)
        nyse = get_calendar(market_code)
        schedule = nyse.schedule(start_date=dt, end_date=dt + timedelta(days=10))
        next_trading_date = schedule[schedule.index > dt].iloc[0].market_open
        delta = next_trading_date - dt
        minutes_until_open = delta.total_seconds() // 60
        return int(minutes_until_open)

    @staticmethod
    def get_minutes_until_market_close(dt: Union[datetime, None] = None, market_code: str = 'XNYS') -> int:
        """Calculate the minutes until the current trading session closes.

        Args:
            dt : datetime, optional
                The current datetime (default is now in UTC).
            market_code : str, optional
                The market code (default is 'XNYS').

        Returns:
            int
                The minutes until the market closes.
                Returns -1 if not currently in the regular trading hours of a market session
                or if it is a market holiday/weekend.

        Examples:
            >>> MarketCalendar.get_minutes_until_market_close()
            180
        """
        if dt is None:
            dt = datetime.now(timezone.utc)
        nyse = get_calendar(market_code)
        if not nyse.is_session(dt):
            return -1
        schedule = nyse.schedule(start_date=dt - timedelta(days=1), end_date=dt + timedelta(days=1))
        current_session = schedule[schedule.index.date == dt.date()]
        if current_session.empty:
            return -1
        market_close = current_session.iloc[0].market_close
        if dt >= market_close:
            return -1
        delta = market_close - dt
        minutes_until_close = delta.total_seconds() // 60
        return int(minutes_until_close)

    @staticmethod
    def get_minutes_until_next_session_open(dt: Union[datetime, None] = None, market_code: str = 'XNYS') -> int:
        """Calculate the minutes until the next session opens.

        Args:
            dt : datetime, optional
                The current datetime (default is now in UTC).
            market_code : str, optional
                The market code (default is 'XNYS').

        Returns:
            int
                The minutes until the next session opens.

        Examples:
            >>> MarketCalendar.get_minutes_until_next_session_open()
            1440
        """
        if dt is None:
            dt = datetime.now(timezone.utc)
        nyse = get_calendar(market_code)
        schedule = nyse.schedule(start_date=dt, end_date=dt + timedelta(days=10))
        next_session = schedule[schedule.index > dt].iloc[0].market_open
        delta = next_session - dt
        minutes_until_next_open = delta.total_seconds() // 60
        return int(minutes_until_next_open)

    @staticmethod
    def get_minutes_until_next_session_close(dt: Union[datetime, None] = None, market_code: str = 'XNYS') -> int:
        """Calculate the minutes until the next session closes.

        Args:
            dt : datetime, optional
                The current datetime (default is now in UTC).
            market_code : str, optional
                The market code (default is 'XNYS').

        Returns:
            int
                The minutes until the next session closes.

        Examples:
            >>> MarketCalendar.get_minutes_until_next_session_close()
            1500
        """
        if dt is None:
            dt = datetime.now(timezone.utc)
        nyse = get_calendar(market_code)
        schedule = nyse.schedule(start_date=dt, end_date=dt + timedelta(days=10))
        next_session = schedule[schedule.index > dt].iloc[0].market_close
        delta = next_session - dt
        minutes_until_next_close = delta.total_seconds() // 60
        return int(minutes_until_next_close)

    @staticmethod
    def calc_trading_minutes_between(start: datetime, end: datetime, market_code: str = 'XNYS') -> int:
        """Calculate the trading minutes between two datetimes.

        Args:
            start : datetime
                The start datetime.
            end : datetime
                The end datetime.
            market_code : str, optional
                The market code (default is 'XNYS').

        Returns:
            int
                The trading minutes between the two datetimes.

        Examples:
            >>> start = datetime(2023, 3, 10, 14, 30, tzinfo=timezone.utc)
            >>> end = datetime(2023, 3, 10, 21, 0, tzinfo=timezone.utc)
            >>> MarketCalendar.calc_trading_minutes_between(start, end)
            390
        """
        nyse = get_calendar(market_code)
        schedule = nyse.schedule(start_date=start, end_date=end)
        trading_minutes = 0
        for _, session in schedule.iterrows():
            market_open = session["market_open"]
            market_close = session["market_close"]
            if start < market_open:
                start = market_open
            if end > market_close:
                end = market_close
            trading_minutes += (end - start).seconds // 60
        return trading_minutes

    @staticmethod
    def calc_trading_days_between(start: datetime, end: datetime, market_code: str = 'XNYS') -> int:
        """Calculate the trading days between two datetimes.

        Args:
            start : datetime
                The start datetime.
            end : datetime
                The end datetime.
            market_code : str, optional
                The market code (default is 'XNYS').

        Returns:
            int
                The trading days between the two datetimes.

        Examples:
            >>> start = datetime(2023, 3, 10, tzinfo=timezone.utc)
            >>> end = datetime(2023, 3, 15, tzinfo=timezone.utc)
            >>> MarketCalendar.calc_trading_days_between(start, end)
            3
        """
        nyse = get_calendar(market_code)
        schedule = nyse.schedule(start_date=start, end_date=end)
        trading_days = len(schedule)
        return trading_days

    @staticmethod
    def calc_trading_minutes_until(end: datetime, dt: Union[datetime, None] = None, market_code: str = 'XNYS') -> int:
        """Calculate the trading minutes until a given datetime.

        Args:
            end : datetime
                The end datetime.
            dt : datetime, optional
                The current datetime (default is now in UTC).
            market_code : str, optional
                The market code (default is 'XNYS').

        Returns:
            int
                The trading minutes until the given datetime.

        Examples:
            >>> end = datetime(2023, 3, 10, 21, 0, tzinfo=timezone.utc)
            >>> MarketCalendar.calc_trading_minutes_until(end)
            390
        """
        if dt is None:
            dt = datetime.now(timezone.utc)
        return MarketCalendar.calc_trading_minutes_between(dt, end, market_code)

    @staticmethod
    def calc_trading_days_until(end: datetime, dt: Union[datetime, None] = None, market_code: str = 'XNYS') -> int:
        """Calculate the trading days until a given datetime.

        Args:
            end : datetime
                The end datetime.
            dt : datetime, optional
                The current datetime (default is now in UTC).
            market_code : str, optional
                The market code (default is 'XNYS').

        Returns:
            int
                The trading days until the given datetime.

        Examples:
            >>> end = datetime(2023, 3, 15, tzinfo=timezone.utc)
            >>> MarketCalendar.calc_trading_days_until(end)
            3
        """
        if dt is None:
            dt = datetime.now(timezone.utc)
        return MarketCalendar.calc_trading_days_between(dt, end, market_code)

    @staticmethod
    def get_last_session_date(dt: Union[datetime, None] = None, market_code: str = 'XNYS') -> date:
        """Get the last session's date.

        Args:
            dt : datetime, optional
                The current datetime (default is now in UTC).
            market_code : str, optional
                The market code (default is 'XNYS').

        Returns:
            date
                The last session's date.

        Examples:
            >>> MarketCalendar.get_last_session_date()
            datetime.date(2023, 3, 10)
        """
        last_session_open = MarketCalendar.get_last_session_open(dt, market_code)
        return last_session_open.date()

    @staticmethod
    def get_next_session_date(dt: Union[datetime, None] = None, market_code: str = 'XNYS') -> date:
        """Get the next session's date.

        Args:
            dt : datetime, optional
                The current datetime (default is now in UTC).
            market_code : str, optional
                The market code (default is 'XNYS').

        Returns:
            date
                The next session's date.

        Examples:
            >>> MarketCalendar.get_next_session_date()
            datetime.date(2023, 3, 13)
        """
        next_session_open = MarketCalendar.get_next_session_open(dt, market_code)
        return next_session_open.date()

    @staticmethod
    def is_regular_trading_hours(dt: Union[datetime, None] = None, market_code: str = 'XNYS') -> bool:
        """Check if the current time is within regular trading hours (RTH).

        Args:
            dt : datetime, optional
                The current datetime (default is now in UTC).
            market_code : str, optional
                The market code (default is 'XNYS').

        Returns:
            bool
                True if the current time is within RTH, False otherwise.

        Examples:
            >>> MarketCalendar.is_regular_trading_hours()
            False
        """
        if dt is None:
            dt = datetime.now(timezone.utc)
        nyse = get_calendar(market_code)
        schedule = nyse.schedule(start_date=dt - timedelta(days=1), end_date=dt + timedelta(days=1))
        current_session = schedule[schedule.index.date == dt.date()]
        if current_session.empty:
            return False
        market_open = current_session.iloc[0].market_open
        market_close = current_session.iloc[0].market_close
        return market_open <= dt <= market_close


__all__ = [
    "TradingDay",
    "MarketCalendar",
]
