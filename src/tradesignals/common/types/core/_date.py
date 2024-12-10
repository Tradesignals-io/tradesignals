# flake8: noqa
"""
Custom type for Date objects in the TradeSignals database.
-----------------------------------------------------

This module contains a custom type for Date objects in the TradeSignals database.
The Date type is a string that stores a date value in ISO 8601 string format for serialization.

### Example Usage

Date objects can be converted to date, datetime, and int values
using the `to_date`, `to_datetime`, and `to_int` methods.

```python
# Create an Date from a date
Date.from_date(date(2023, 1, 1))

# Create an Date from a datetime
Date.from_datetime(datetime(2023, 1, 1))

# Create an Date from an int (timestamp in seconds)
Date.from_int(1672531200)

# Create an Date from a string
Date('2023-01-01')

# Serialize to JSON from any of the above

# date
json.dumps(Date.from_date(date(2023, 1, 1)))

# datetime
json.dumps(Date.from_datetime(datetime(2023, 1, 1)))

# int
json.dumps(Date.from_int(1672531200))

# str
json.dumps(Date('2023-01-01'))
```

In JSON, the value will be stored as a string to ensure consistency:

```json
{
    "iso_date_str": "2023-01-01"
}
```
"""
from datetime import date, datetime
from typing import Union


class DateT:
    """
    ## DateT (Union[date, datetime, int, str])
    DateT is a Union[date, datetime, int, str] that stores a date value
    in ISO 8601 string format for serialization.

    ### Example Usage

    DateT objects can be converted to date, datetime, and int values
    using the `to_date`, `to_datetime`, and `to_int` methods.

    ```python
    # Create an DateT from a date
    DateT.from_date(date(2023, 1, 1))

    # Create an DateT from a datetime
    DateT.from_datetime(datetime(2023, 1, 1))

    # Create an DateT from an int (timestamp in seconds)
    DateT.from_int(1672531200)

    # Create an DateT from a string
    DateT('2023-01-01')

    # Serialize to JSON from any of the above

    # date
    json.dumps(DateT.from_date(date(2023, 1, 1)))

    # datetime
    json.dumps(DateT.from_datetime(datetime(2023, 1, 1)))

    # int
    json.dumps(DateT.from_int(1672531200))

    # str
    json.dumps(DateT('2023-01-01'))
    ```

    In JSON, the value will be stored as a string to ensure consistency:

    ```json
    {
        "iso_date_str": "2023-01-01"
    }
    ```
    """

    def __init__(self, value: Union[date, datetime, int, str]):
        """
        Initializes the DateT object.

        Args:
            value (str): The string representation of the ISO date.
        """
        if isinstance(value, date):
            self.value = value.isoformat()
        elif isinstance(value, datetime):
            self.value = value.date().isoformat()
        elif isinstance(value, int):
            dt = datetime.fromtimestamp(value)
            self.value = dt.date().isoformat()
        elif isinstance(value, str):
            self.value = value
        else:
            raise TypeError("Unsupported type for DateT")

    @classmethod
    def from_date(cls, dt: date) -> 'DateT':
        """
        Creates an DateT object from a date object.

        Args:
            dt (date): The date object to convert.

        Returns:
            DateT: A new DateT object.
        """
        return cls(dt.isoformat())

    @classmethod
    def from_datetime(cls, dt: datetime) -> 'DateT':
        """
        Creates an DateT object from a datetime object.

        Args:
            dt (datetime): The datetime object to convert.

        Returns:
            DateT: A new DateT object.
        """
        return cls(dt.date().isoformat())

    @classmethod
    def from_int(cls, timestamp: int) -> 'DateT':
        """
        Creates an DateT object from an integer timestamp.

        Args:
            timestamp (int): The timestamp to convert (in seconds).

        Returns:
            DateT: A new DateT object.
        """
        dt = datetime.fromtimestamp(timestamp).date()
        return cls(dt.isoformat())

    def to_date(self) -> date:
        """
        Converts the string value to a date object.

        Returns:
            date: The date representation of the value.
        """
        return date.fromisoformat(self.value)

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
        Returns the string representation of the DateT object.

        Returns:
            str: The string representation of the value.
        """
        return self.value

    def __repr__(self) -> str:
        """
        Returns the string representation of the DateT object.

        Returns:
            str: The string representation of the value.
        """
        return f"DateT({self.value})"

    def __eq__(self, other: Union['DateT', date, datetime, int, str]) -> bool:
        """
        Checks if the current DateT is equal to another.

        Args:
            other (Union[DateT, date, datetime, int, str]): The other DateT object to compare.

        Returns:
            bool: True if the current DateT is equal to the other, False otherwise.
        """
        if not isinstance(other, DateT):
            other = DateT(other)
        return self.to_date() == other.to_date()

    def __ge__(self, other: Union['DateT', date, datetime, int, str]) -> bool:
        """
        Checks if the current DateT is greater than or equal to another.

        Args:
            other (Union[DateT, date, datetime, int, str]): The other DateT object to compare.

        Returns:
            bool: True if the current DateT is greater than or equal to the other, False otherwise.
        """
        if not isinstance(other, DateT):
            other = DateT(other)
        return self.to_date() >= other.to_date()

    def __le__(self, other: Union['DateT', date, datetime, int, str]) -> bool:
        """
        Checks if the current DateT is less than or equal to another.

        Args:
            other (Union[DateT, date, datetime, int, str]): The other DateT object to compare.

        Returns:
            bool: True if the current DateT is less than or equal to the other, False otherwise.
        """
        if not isinstance(other, DateT):
            other = DateT(other)
        return self.to_date() <= other.to_date()

    def __ne__(self, other: Union['DateT', date, datetime, int, str]) -> bool:
        """
        Checks if the current DateT is not equal to another.

        Args:
            other (Union[DateT, date, datetime, int, str]): The other DateT object to compare.

        Returns:
            bool: True if the current DateT is not equal to the other, False otherwise.
        """
        if not isinstance(other, DateT):
            other = DateT(other)
        return self.to_date() != other.to_date()

    def __gt__(self, other: Union['DateT', date, datetime, int, str]) -> bool:
        """
        Checks if the current DateT is greater than another.

        Args:
            other (Union[DateT, date, datetime, int, str]): The other DateT object to compare.

        Returns:
            bool: True if the current DateT is greater than the other, False otherwise.
        """
        if not isinstance(other, DateT):
            other = DateT(other)
        return self.to_date() > other.to_date()

    def __lt__(self, other: Union['DateT', date, datetime, int, str]) -> bool:
        """
        Checks if the current DateT is less than another.

        Args:
            other (Union[DateT', date, datetime, int, str]): The other DateT object to compare.

        Returns:
            bool: True if the current DateT is less than the other, False otherwise.
        """
        if not isinstance(other, DateT):
            other = DateT(other)
        return self.to_date() < other.to_date()

    def __json__(self) -> str:
        """
        Serializes the DateT object to a JSON-compatible string.

        Returns:
            str: The JSON-compatible string representation of the value.
        """
        return self.value
