# flake8: noqa
"""
Custom types for fields in the TradeSignals database.
-----------------------------------------------------

This module contains custom types for fields in the TradeSignals database.
These types are used to ensure consistency and type safety in the database.
"""
from datetime import datetime
from typing import Union


class DateTimeT:
    """
    ## DateTimeT (Union[datetime, int, str])
    DateTimeT is a Union[datetime, int, str] that stores a datetime value
    in ISO 8601 string format for serialization.

    ### Example Usage

    DateTimeT objects can be converted to datetime and int values
    using the `to_datetime` and `to_int` methods.

    ```python
    # Create a DateTimeT from a datetime
    DateTimeT.from_datetime(datetime(2023, 1, 1, 12, 0, 0))

    # Create a DateTimeT from an int (timestamp in seconds)
    DateTimeT.from_int(1672531200)

    # Create a DateTimeT from a string
    DateTimeT('2023-01-01T12:00:00')

    # Serialize to JSON from any of the above

    # datetime
    json.dumps(DateTimeT.from_datetime(datetime(2023, 1, 1, 12, 0, 0)))

    # int
    json.dumps(DateTimeT.from_int(1672531200))

    # str
    json.dumps(DateTimeT('2023-01-01T12:00:00'))
    ```

    In JSON, the value will be stored as a string to ensure consistency:

    ```json
    {
        "iso_datetime_str": "2023-01-01T12:00:00"
    }
    ```
    """

    def __init__(self, value: Union[datetime, int, str]):
        """
        Initializes the DateTimeT object.

        Args:
            value (str): The string representation of the ISO datetime.
        """
        if isinstance(value, datetime):
            self.value = value.isoformat()
        elif isinstance(value, int):
            dt = datetime.fromtimestamp(value)
            self.value = dt.isoformat()
        elif isinstance(value, str):
            self.value = value
        else:
            raise TypeError("Unsupported type for DateTimeT")

    @classmethod
    def from_datetime(cls, dt: datetime) -> 'DateTimeT':
        """
        Creates a DateTimeT object from a datetime object.

        Args:
            dt (datetime): The datetime object to convert.

        Returns:
            DateTimeT: A new DateTimeT object.
        """
        return cls(dt.isoformat())

    @classmethod
    def from_int(cls, timestamp: int) -> 'DateTimeT':
        """
        Creates a DateTimeT object from an integer timestamp.

        Args:
            timestamp (int): The timestamp to convert (in seconds).

        Returns:
            DateTimeT: A new DateTimeT object.
        """
        dt = datetime.fromtimestamp(timestamp)
        return cls(dt.isoformat())

    def to_datetime(self) -> datetime:
        """
        Converts the string value to a datetime object.

        Returns:
            datetime: The datetime representation of the value.
        """
        return datetime.fromisoformat(self.value)

    def to_int(self) -> int:
        """
        Converts the string value to an integer timestamp (in seconds).

        Returns:
            int: The integer representation of the value.
        """
        dt = self.to_datetime()
        return int(dt.timestamp())

    def __str__(self) -> str:
        """
        Returns the string representation of the DateTimeT object.

        Returns:
            str: The string representation of the value.
        """
        return self.value

    def __repr__(self) -> str:
        """
        Returns the string representation of the DateTimeT object.

        Returns:
            str: The string representation of the value.
        """
        return f"DateTimeT({self.value})"

    def __eq__(self, other: Union['DateTimeT', datetime, int, str]) -> bool:
        """
        Checks if the current DateTimeT is equal to another.

        Args:
            other (Union[DateTimeT, datetime, int, str]): The other DateTimeT object to compare.

        Returns:
            bool: True if the current DateTimeT is equal to the other, False otherwise.
        """
        if not isinstance(other, DateTimeT):
            other = DateTimeT(other)
        return self.to_datetime() == other.to_datetime()

    def __ge__(self, other) -> bool:
        """
        Checks if the current DateTimeT is greater than or equal to another.

        Args:
            other (DateTimeT): The other DateTimeT object to compare.

        Returns:
            bool: True if the current DateTimeT is greater than or equal to the other, False otherwise.
        """
        return self.to_datetime() >= other.to_datetime()

    def __le__(self, other) -> bool:
        """
        Checks if the current DateTimeT is less than or equal to another.

        Args:
            other (DateTimeT): The other DateTimeT object to compare.

        Returns:
            bool: True if the current DateTimeT is less than or equal to the other, False otherwise.
        """
        return self.to_datetime() <= other.to_datetime()

    def __ne__(self, other) -> bool:
        """
        Checks if the current DateTimeT is not equal to another.

        Args:
            other (DateTimeT): The other DateTimeT object to compare.

        Returns:
            bool: True if the current DateTimeT is not equal to the other, False otherwise.
        """
        return self.to_datetime() != other.to_datetime()

    def __gt__(self, other) -> bool:
        """
        Checks if the current DateTimeT is greater than another.

        Args:
            other (DateTimeT): The other DateTimeT object to compare.

        Returns:
            bool: True if the current DateTimeT is greater than the other, False otherwise.
        """
        return self.to_datetime() > other.to_datetime()

    def __lt__(self, other) -> bool:
        """
        Checks if the current DateTimeT is less than another.

        Args:
            other (DateTimeT): The other DateTimeT object to compare.

        Returns:
            bool: True if the current DateTimeT is less than the other, False otherwise.
        """
        return self.to_datetime() < other.to_datetime()

    def __json__(self) -> str:
        """
        Serializes the DateTimeT object to a JSON-compatible string.

        Returns:
            str: The JSON-compatible string representation of the value.
        """
        return self.value


__all__ = [

    "DateTimeT",
]
