from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Literal, Optional, Union

TimestampResolution = Literal["s", "ms", "ns"]


class TimestampT:
    """
    Initialize a new TimestampT object.

    Args:
        value (Union[int, str, datetime, date, Decimal]): The timestamp value to initialize with.
            - int: Unix timestamp in seconds, milliseconds or nanoseconds
            - str: ISO format datetime string
            - datetime: Python datetime object
            - date: Python date object
            - Decimal: Decimal number representing seconds since epoch
        tz (Optional[timezone]): Timezone for the timestamp. Defaults to UTC.
        resolution (Optional[TimestampResolution]): Explicitly set the timestamp resolution.
            Can be 's' (seconds), 'ms' (milliseconds) or 'ns' (nanoseconds).

    Raises:
        ValueError: If the value type is invalid or the integer length is incorrect
    """

    def __init__(self, value: Union[int, str, datetime, date, Decimal], tz: Optional[timezone]=None, resolution: Optional[TimestampResolution]=None):
        """
        Create a new TimestampT object.

        Args:
            value (Union[int, str, datetime, date, Decimal]): The timestamp value to initialize with.
            tz (Optional[timezone]): Timezone for the timestamp. Defaults to UTC.
            resolution (Optional[TimestampResolution]): Explicitly set the timestamp resolution.
                Can be 's' (seconds), 'ms' (milliseconds) or 'ns' (nanoseconds).

        Raises:
            ValueError: If the value type is invalid or the integer length is incorrect
        """
        self.tz = timezone.utc if tz is None else tz
        self.resolution = resolution

        if isinstance(value, date):
            self.timestamp = datetime(value.year, value.month, value.day, 0, 0, 0, 0, tzinfo=self.tz)
            self.resolution = "ms" if resolution is None else resolution
        elif isinstance(value, datetime):
            self.timestamp = value
            if self.resolution is None:
                if value.microsecond > 0:
                    self.resolution = "ns" if value.microsecond % 1000 != 0 else "ms"
                else:
                    self.resolution = "s"
        elif isinstance(value, int):
            length = len(str(value))
            if length <= 10:  # seconds
                self.timestamp = datetime.fromtimestamp(value, tz=self.tz)
                self.resolution = "s"
            elif length <= 13:  # milliseconds
                self.timestamp = datetime.fromtimestamp(value / 1000, tz=self.tz)
                self.resolution = "ms"
            elif length <= 19:  # nanoseconds
                self.timestamp = datetime.fromtimestamp(value / 1e9, tz=self.tz)
                self.resolution = "ns"
            else:
                raise ValueError("Invalid int length for TimestampT")
        elif isinstance(value, str):
            self.timestamp = datetime.fromisoformat(value)
            if self.resolution is None:
                if self.timestamp.microsecond > 0:
                    self.resolution = "ns" if self.timestamp.microsecond % 1000 != 0 else "ms"
                else:
                    self.resolution = "s"
        elif isinstance(value, Decimal):
            self.timestamp = datetime.fromtimestamp(float(value), tz=self.tz)
            self.resolution = "ms"
        else:
            raise ValueError("Invalid value type for TimestampT")

    @staticmethod
    def now(tz: Optional[timezone] = timezone.utc) -> "TimestampT":
        """
        Create a TimestampT object with the current time.

        Args:
            tz (Optional[timezone]): Timezone for the timestamp. Defaults to UTC.

        Returns:
            TimestampT: A new TimestampT object representing the current time
        """
        return TimestampT(datetime.now(tz=tz))

    @classmethod
    def difference(cls, ts1: "TimestampT", ts2: "TimestampT", unit: str = "seconds") -> float:
        """
        Calculate the time difference between two timestamps.

        Args:
            ts1 (TimestampT): First timestamp
            ts2 (TimestampT): Second timestamp
            unit (str): Unit for the result. One of:
                'ms' (milliseconds)
                'ns' (nanoseconds)
                'seconds'
                'minutes'
                'hours'
                'days'

        Returns:
            float: Time difference in the specified unit

        Raises:
            ValueError: If an invalid unit is specified
        """
        delta = ts2.timestamp - ts1.timestamp
        if unit == "ms":
            return delta.total_seconds() * 1000
        elif unit == "ns":
            return delta.total_seconds() * 1e9
        elif unit == "seconds":
            return delta.total_seconds()
        elif unit == "minutes":
            return delta.total_seconds() / 60
        elif unit == "hours":
            return delta.total_seconds() / 3600
        elif unit == "days":
            return delta.total_seconds() / 86400
        else:
            raise ValueError("Invalid unit. Choose from 'ms', 'ns', 'seconds', 'minutes', 'hours', or 'days'.")

    def to_datetime(self) -> datetime:
        """
        Convert to Python datetime object.

        Returns:
            datetime: The timestamp as a datetime object
        """
        return self.timestamp

    def to_pretty_string(self) -> str:
        """
        Format timestamp as a pretty string.

        Returns:
            str: Formatted string like "2023-12-31 23:59:59.999"
        """
        return self.timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    def to_nanos(self) -> int:
        """
        Convert to nanoseconds since epoch.

        Returns:
            int: Timestamp in nanoseconds
        """
        return int(self.timestamp.timestamp() * 1e9)

    def to_micros(self) -> int:
        """
        Convert to microseconds since epoch.

        Returns:
            int: Timestamp in microseconds
        """
        return int(self.timestamp.timestamp() * 1e6)

    def to_millis(self) -> int:
        """
        Convert to milliseconds since epoch.

        Returns:
            int: Timestamp in milliseconds
        """
        return int(self.timestamp.timestamp() * 1000)

    def to_seconds(self) -> int:
        """
        Convert to seconds since epoch.

        Returns:
            int: Timestamp in seconds
        """
        return int(self.timestamp.timestamp())

    def to_iso_string(self) -> str:
        """
        Convert to ISO 8601 format string.

        Returns:
            str: ISO formatted datetime string
        """
        return self.timestamp.isoformat()

    def pretty_datetime(self) -> str:
        """
        Format as pretty date and time.

        Returns:
            str: Formatted string like "31 dec 2023 11:59pm"
        """
        return self.timestamp.strftime("%d %b %Y %-I:%M%p").lower()

    def pretty_time(self) -> str:
        """
        Format as pretty time.

        Returns:
            str: Formatted string like "11:59pm"
        """
        return self.timestamp.strftime("%-I:%M%p").lower()

    def pretty_date(self) -> str:
        """
        Format as pretty date.

        Returns:
            str: Formatted string like "31 dec 2023"
        """
        return self.timestamp.strftime("%d %b %Y").lower()

    def pretty_timedelta(self) -> str:
        """
        Get a human readable time difference from now.

        Returns:
            str: Relative time string like "5 minutes ago" or "just now"
        """
        now = datetime.now(tz=self.timestamp.tzinfo)
        delta = now - self.timestamp
        days = delta.days
        seconds = delta.seconds
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60

        if days > 0:
            return f"{days} day{'s' if days > 1 else ''} ago"
        elif hours > 0:
            return f"{hours} hour{'s' if hours > 1 else ''} ago"
        elif minutes > 0:
            return f"{minutes} minute{'s' if minutes > 1 else ''} ago"
        else:
            return "just now"

    def __str__(self) -> str:
        """
        Get string representation.

        Returns:
            str: Formatted datetime string
        """
        return self.timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")

    def __repr__(self) -> str:
        """
        Get detailed string representation.

        Returns:
            str: String showing class name and timestamp
        """
        return f"TimestampT(timestamp={self.timestamp})"

    def __json__(self) -> int:
        """
        JSON serialization support.

        Returns:
            int: Timestamp in milliseconds for JSON encoding
        """
        return int(self.timestamp.timestamp() * 1000)

    def __serialize__(self) -> int:
        """
        Serialization support.

        Returns:
            int: Timestamp in milliseconds for serialization
        """
        return int(self.timestamp.timestamp() * 1000)

    def __hash__(self) -> int:
        """
        Get hash value.

        Returns:
            int: Hash of internal timestamp
        """
        return hash(self.timestamp)

    def __deepcopy__(self, memo) -> "TimestampT":
        """
        Deep copy support.

        Returns:
            TimestampT: A deep copy of this object
        """
        return TimestampT(self.timestamp)

    def __copy__(self) -> "TimestampT":
        """
        Shallow copy support.

        Returns:
            TimestampT: A shallow copy of this object
        """
        return TimestampT(self.timestamp)

    def __deserialize__(self, data: str) -> "TimestampT":
        """
        Deserialize a TimestampT object from a string.

        Args:
            data (str): The string representation of the timestamp.

        Returns:
            TimestampT: A new TimestampT object.
        """
        return TimestampT(datetime.fromisoformat(data))

    def __ge__(self, other: Union['TimestampT', int, str, datetime, date]) -> bool:
        """
        Greater than or equal comparison.

        Args:
            other: Value to compare against

        Returns:
            bool: True if this timestamp is >= other
        """
        if not isinstance(other, TimestampT):
            other = TimestampT(other)
        return self.timestamp >= other.timestamp

    def __le__(self, other: Union['TimestampT', int, str, datetime, date]) -> bool:
        """
        Less than or equal comparison.

        Args:
            other: Value to compare against

        Returns:
            bool: True if this timestamp is <= other
        """
        if not isinstance(other, TimestampT):
            other = TimestampT(other)
        return self.timestamp <= other.timestamp

    def __eq__(self, other: Union['TimestampT', int, str, datetime, date]) -> bool:
        """
        Equality comparison.

        Args:
            other: Value to compare against

        Returns:
            bool: True if timestamps are equal
        """
        if not isinstance(other, TimestampT):
            other = TimestampT(other)
        return self.timestamp == other.timestamp

    def __ne__(self, other: Union['TimestampT', int, str, datetime, date]) -> bool:
        """
        Not equal comparison.

        Args:
            other: Value to compare against

        Returns:
            bool: True if timestamps are not equal
        """
        if not isinstance(other, TimestampT):
            other = TimestampT(other)
        return self.timestamp != other.timestamp

    def __gt__(self, other: Union['TimestampT', int, str, datetime, date]) -> bool:
        """
        Greater than comparison.

        Args:
            other: Value to compare against

        Returns:
            bool: True if this timestamp is > other
        """
        if not isinstance(other, TimestampT):
            other = TimestampT(other)
        return self.timestamp > other.timestamp

    def __lt__(self, other: Union['TimestampT', int, str, datetime, date]) -> bool:
        """
        Less than comparison.

        Args:
            other: Value to compare against

        Returns:
            bool: True if this timestamp is < other
        """
        if not isinstance(other, TimestampT):
            other = TimestampT(other)
        return self.timestamp < other.timestamp


__all__ = [
    "TimestampT",
    "TimestampResolution",
]
