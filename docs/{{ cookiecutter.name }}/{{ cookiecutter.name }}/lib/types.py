"""Module for handling fixed precision decimal numbers.

This module provides classes for handling fixed precision decimal numbers.
The BaseDecimalT class extends Decimal to provide fixed scale and precision handling.
"""

import logging
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from enum import Enum, unique
from typing import Union

from attrs import define, field
from cattrs import unstructure

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

__all__ = [
    "BaseDecimalT",
    "PriceDecimalT",
    "FractionDecimalT",
    "TimestampT",
    "Side",
    "Bias",
    "ExpirationDateT",
    "OptionSymbolT",
    "PutCall",
    "BaseDecimalT",
    "TimestampResolution",
]


class PutCall(Enum):
    """The put or call of the option."""
    PUT = "P"
    CALL = "C"


FIXED_PRICE_SCALE = 6


class PriceDataT(Decimal):
    """Class for transporting fixed precision decimals."""

    def __init__(self, value: str, scale: int = FIXED_PRICE_SCALE) -> None:
        """Initialize the PriceDecimalT object.

        Args:
            value : str
                The price value as a string.
            scale : int
                The scale of the price.
        """
        super().__init__(value)
        self.scale = scale

    def __new__(cls, value: str, scale: int = FIXED_PRICE_SCALE) -> "PriceDataT":
        """Create a new PriceDecimalT object.

        Args:
            value : str
                The price value as a string.
            scale : int
                The scale of the price.

        Returns:
            PriceDecimalT
                The new PriceDecimalT object.
        """
        self = super().__new__(cls, value)
        self.scale = scale
        return self

    def set_scale(self, scale: int = FIXED_PRICE_SCALE) -> "PriceDataT":
        """Set the scale of the PriceDecimalT object.

        Args:
            scale : int
                The scale of the price.

        Returns:
            PriceDecimalT
                The PriceDecimalT object with the new scale.
        """
        self.scale = scale
        return self

    @classmethod
    def from_int(cls, price_int: int, scale: int = FIXED_PRICE_SCALE) -> "PriceDataT":
        """Convert a Databento price integer to a PriceDecimalT object.

        Args:
            price_int : int
                The price as an integer.
            scale : int
                The scale of the price.

        Returns:
            PriceDecimalT
                The price as a PriceDecimalT object.
        """
        instance = cls(str(price_int / 10**scale))
        instance.scale = scale
        return instance

    def to_int(self, scale: int = FIXED_PRICE_SCALE) -> int:
        """Convert the PriceDecimalT to an integer.

        Args:
            scale : int
                The scale of the price.

        Returns:
            int
                The price as an integer scaled to 1e6.
        """
        return int(self * Decimal(10**scale))

    def to_bytes(self, scale: int = FIXED_PRICE_SCALE) -> bytes:
        """Convert PriceDataT value to avro bytes.

        Args:
            scale : int
                The scale of the price.

        Returns:
            bytes
                The price serialized into Avro bytes with precision of 18 and
                scale of 6.

        Raises:
            ValueError
                If the price integer is invalid.
            ArithmeticError
                If there is an arithmetic error during conversion.
            InvalidOperation
                If the decimal operation is invalid.
            UnicodeDecodeError
                If the avro_bytes cannot be decoded to a string.
            OverflowError
                If the decimal value is too large to be represented.
        """
        try:
            getcontext().prec = 18
            price_decimal = self / Decimal(10**scale)
            return price_decimal.quantize(Decimal("1.000000")).to_eng_string().encode("utf-8")
        except (ValueError, ArithmeticError, InvalidOperation) as error:
            raise ValueError(f"Error converting price to bytes: {error}") from error

    @classmethod
    def from_bytes(cls, avro_bytes: bytes, scale: int = FIXED_PRICE_SCALE) -> "PriceDataT":
        """Convert Avro bytes to PriceDataT.

        Args:
            avro_bytes : bytes
                The Avro bytes representing the price.
            scale : int
                The scale of the price.

        Returns:
            PriceDataT
                The price as a PriceDataT object.

        Raises:
            ValueError
                If the Avro bytes cannot be converted to a valid PriceDataT.
            ArithmeticError
                If there is an arithmetic error during conversion.
            InvalidOperation
                If the decimal operation is invalid.
            UnicodeDecodeError
                If the avro_bytes cannot be decoded to a string.
            OverflowError
                If the decimal value is too large to be represented.
        """
        try:
            price_str = avro_bytes.decode("utf-8")
            price_decimal = Decimal(price_str).quantize(Decimal("1.000000"))
            return cls(str(price_decimal))
        except (ValueError, ArithmeticError, InvalidOperation) as error:
            raise ValueError(f"Error converting Avro bytes `{avro_bytes}`: {error}") from error


class BaseDecimalT(Decimal):
    """Base class for handling fixed precision decimal numbers."""

    def __new__(cls, value: str, scale: int = 9, precision: int = 22, currency_iso: str = "USD"):
        """Initialize with value, scale and precision.

        Args:
            value : str
                The decimal value as a string
            scale : int
                Number of decimal places
            precision : int
                Total number of significant digits
            currency_iso : str, optional
                The ISO code for the currency (default: "USD")

        Raises:
            ValueError
                If the value is not a string
            TypeError
                If scale, precision are not integers or currency_iso not a string
            InvalidOperation
                If the value cannot be converted to a Decimal
        """
        if not isinstance(value, str):
            raise ValueError("value must be a string")
        if not isinstance(scale, int):
            raise TypeError("scale must be an integer")
        if not isinstance(precision, int):
            raise TypeError("precision must be an integer")
        if not isinstance(currency_iso, str):
            raise TypeError("currency_iso must be a string")

        try:
            logger.info(f"Creating BaseDecimalT with value: {value=}, scale: {scale=}, precision: {precision=}, currency_iso: {currency_iso=}")
            self = super().__new__(cls, value)
        except InvalidOperation as e:
            raise ValueError(f"Invalid decimal value: {value}, {e}") from e

        self._scale = scale
        self._precision = precision
        self._currency_iso = currency_iso
        self._currency_symbol = self._symbol_map.get(currency_iso, currency_iso)
        return self

    def to_avro_bytes(self, scale_override: int | None = None) -> bytes:
        """Convert the decimal to Avro-compatible bytes.

        Args:
            scale_override : int | None, optional
                Override the scale of the decimal value

        Returns:
            bytes
                Avro-encoded decimal bytes
        """
        scale_value = scale_override or self._scale
        scaled_value_int = int(self * (10**scale_value))
        byte_length = (scaled_value_int.bit_length() + 8) // 8
        return scaled_value_int.to_bytes(byte_length, byteorder="big", signed=True)

    @classmethod
    def from_avro_bytes(cls, bytes_val: bytes, scale_override: int | None = None) -> 'BaseDecimalT':
        """Convert Avro-compatible bytes back to a decimal.

        Args:
            bytes_val : bytes
                The bytes to convert
            scale_override : int | None, optional
                Override the scale of the decimal value

        Returns:
            BaseDecimalT
                The decimal value
        """
        scale_value = scale_override or cls._scale
        int_val = int.from_bytes(bytes_val, byteorder='big', signed=True)
        return cls(str(Decimal(int_val) / Decimal(10**scale_value)))

    @property
    def _symbol_map(self) -> dict[str, str]:
        """Return currency ISO code to symbol mapping.

        Returns:
            dict[str, str]
                Mapping of currency codes to symbols
        """
        return {
            "USD": "$",
            "EUR": "€",
            "GBP": "£",
        }

    @property
    def round_half_up(self) -> 'BaseDecimalT':
        """Round decimal to 2 places using ROUND_HALF_UP.

        Returns:
            BaseDecimalT
                Rounded decimal value

        Examples:
            >>> BaseDecimalT("123.456789", 9, 22).round_half_up
            BaseDecimalT('123.46')
            >>> BaseDecimalT("123.454", 9, 22).round_half_up
            BaseDecimalT('123.45')
        """
        return self.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

    @property
    def currency_symbol(self) -> str:
        """Get the currency symbol.

        Returns:
            str
                The currency symbol
        """
        return self._currency_symbol

    @property
    def currency_iso(self) -> str:
        """Get the currency ISO code.

        Returns:
            str
                The currency ISO code
        """
        return self._currency_iso

    @property
    def currency_str(self) -> str:
        """Format as currency string.

        Returns:
            str
                Formatted currency string

        Examples:
            >>> BaseDecimalT("123.456789", 9, 22).currency_str
            '$123.46'
        """
        return f"{self.currency_symbol}{float(self):,.2f}"

    def __str__(self) -> str:
        """Get string representation.

        Returns:
            str
                String representation
        """
        return self.currency_str

    def __repr__(self) -> str:
        """Get detailed string representation.

        Returns:
            str
                Detailed string representation
        """
        return f"{self.__class__.__name__}('{self.currency_str}')"

    def __json__(self) -> str:
        """Serialize to JSON string.

        Returns:
            str
                JSON serialized string
        """
        return str(self.to_avro_bytes())


class PriceDecimalT(BaseDecimalT):
    """Class for handling price decimal numbers with 9 decimal places."""

    def __new__(cls, value: str, currency_iso: str = "USD") -> 'PriceDecimalT':
        """
        Initialize price decimal.

        Args:
            value : str
                The decimal value as string
            currency_iso : str, optional
                Currency ISO code (default: USD)

        Returns:
            PriceDecimalT
                The decimal value
        """
        instance = super().__new__(cls, value, 9, 22, currency_iso)
        return instance

    @classmethod
    def from_scaled_int(cls, value: int, scale: int = 9) -> 'PriceDecimalT':
        """
        Create a PriceDecimalT from a scaled integer.

        Args:
            value: int
                The scaled integer value
            scale: int
                The scale of the integer value

        Returns:
            PriceDecimalT
                The decimal value
        """
        decimal_value = Decimal(str(Decimal(value) / Decimal(10**scale)))
        return cls(str(decimal_value))


class FractionDecimalT(BaseDecimalT):
    """Class for handling fractional decimal numbers with 9 decimal places."""

    def __new__(cls, value: str, currency_iso: str = "USD") -> 'FractionDecimalT':
        """
        Initialize fractional decimal.

        Args:
            value : str
                The decimal value as string
            currency_iso : str, optional
                Currency ISO code (default: USD)

        Returns:
            FractionDecimalT
                The decimal value
        """
        instance = super().__new__(cls, value, 18, 16, currency_iso)
        return instance


@unique
class TimestampResolution(Enum):
    """
    The resolution of a timestamp.
    """

    SECONDS = "seconds"
    MILLISECONDS = "milliseconds"
    MICROSECONDS = "microseconds"
    NANOSECONDS = "nanoseconds"
    DAYS = "days"


class TimestampT(datetime):
    """
    TimestampT extends datetime to store timestamps with millisecond precision.

    Can be constructed from:
    - date
    - datetime
    - int (with configurable resolution)
    - ISO format string

    Original input value is preserved in _init_value
    """

    def __new__(
        cls,
        value: Union[date, datetime, int, str],
        resolution: TimestampResolution = TimestampResolution.MILLISECONDS
    ) -> 'TimestampT':
        """
        Create new TimestampT instance.

        Args:
            value: Input value as date, datetime, int or ISO string
            resolution: Resolution for int timestamps

        Returns:
            TimestampT
                New TimestampT instance

        Raises:
            TypeError
                If the value cannot be converted to a TimestampT
        """
        # Store original input
        cls._init_value = value

        # Convert to millisecond timestamp
        if isinstance(value, (date, datetime)):
            if isinstance(value, date):
                dt = datetime.combine(value, datetime.min.time())
            else:
                dt = value
            ms = int(dt.timestamp() * 1000)

        elif isinstance(value, int):
            if resolution == TimestampResolution.DAYS:
                ms = value * 86400 * 1000
            elif resolution == TimestampResolution.SECONDS:
                ms = value * 1000
            elif resolution == TimestampResolution.MILLISECONDS:
                ms = value
            elif resolution == TimestampResolution.MICROSECONDS:
                ms = value // 1000
            else:  # nanoseconds
                ms = value // 1_000_000

        elif isinstance(value, str):
            dt = datetime.fromisoformat(value)
            ms = int(dt.timestamp() * 1000)

        else:
            raise TypeError(f"Cannot create TimestampT from {type(value)}")

        # Create datetime instance from milliseconds
        dt = datetime.fromtimestamp(ms / 1000)
        return super().__new__(cls, dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, dt.microsecond)

    @property
    def milliseconds(self) -> int:
        """Get milliseconds since epoch

        Returns:
            int
                Milliseconds since epoch
        """
        return int(self.timestamp() * 1000)

    def to_int(self, resolution: TimestampResolution = TimestampResolution.MILLISECONDS) -> int:
        """
        Convert to integer timestamp at specified resolution

        Args:
            resolution: Target timestamp resolution

        Returns:
            Integer timestamp at specified resolution
        """
        ms = self.milliseconds

        if resolution == TimestampResolution.DAYS:
            return ms // (86400 * 1000)
        elif resolution == TimestampResolution.SECONDS:
            return ms // 1000
        elif resolution == TimestampResolution.MILLISECONDS:
            return ms
        elif resolution == TimestampResolution.MICROSECONDS:
            return ms * 1000
        else:  # nanoseconds
            return ms * 1_000_000

    @classmethod
    def from_int(cls, value: int) -> "TimestampT":
        """
        Create TimestampT from integer timestamp with resolution auto-detected from value length.

        Args:
            value : int
                Integer timestamp value. Resolution is determined by number of digits:
                - 9-10 digits: seconds
                - 12-13 digits: milliseconds
                - 15-16 digits: microseconds
                - 18-19 digits: nanoseconds
                - 1-5 digits: days

        Returns:
            TimestampT
                New TimestampT instance with detected resolution

        Examples:
            ```python
            # Seconds (10 digits)
            ts = TimestampT.from_int(1672531200)

            # Milliseconds (13 digits)
            ts = TimestampT.from_int(1672531200000)

            # Microseconds (16 digits)
            ts = TimestampT.from_int(1672531200000000)
            ```
        """
        digits = len(str(abs(value)))

        if digits >= 18:
            resolution = TimestampResolution.NANOSECONDS
        elif digits >= 15:
            resolution = TimestampResolution.MICROSECONDS
        elif digits >= 12:
            resolution = TimestampResolution.MILLISECONDS
        elif digits >= 9:
            resolution = TimestampResolution.SECONDS
        else:
            resolution = TimestampResolution.DAYS

        return cls(value, resolution=resolution)

    def __str__(self) -> str:
        return self.isoformat()

    def __repr__(self) -> str:
        return f"TimestampT({self.isoformat()})"

    def __json__(self) -> str:
        return self.isoformat()

    def friendly_delta(self, other: "TimestampT") -> str:
        """
        Returns a user friendly string describing the time difference between two timestamps.

        Args:
            other : TimestampT
                The timestamp to compare against

        Returns:
            str
                Human readable time difference string

        Examples:
            ```python
            ts1 = TimestampT.from_int(1672531200)  # Jan 1 2023
            ts2 = TimestampT.from_int(1672617600)  # Jan 2 2023

            ts1.friendly_delta(ts2)  # Returns "1 day ago"
            ```
        """
        if not isinstance(other, TimestampT):
            other = TimestampT(other)

        delta = other - self
        seconds = int(delta.total_seconds())
        minutes = seconds // 60
        hours = minutes // 60
        days = delta.days

        if seconds < 180:  # Less than 3 minutes
            return "just now"

        if days == 1:
            return "yesterday"

        if days > 1:
            return f"{days} days ago"

        if hours >= 12:
            return f"{hours} hours ago"

        if hours > 0:
            remaining_mins = minutes % 60
            return f"{hours} hours {remaining_mins} minutes ago"

        return f"{minutes} minutes ago"


class Side(Enum):
    """
    ----------------------------------------------------------------
    Trade Side string enumeration.
    ----------------------------------------------------------------

    The side of the nbbo bid/ask that the trade occurred on.

    Values
        BID (str): Trade occurred on nearer to, or below, the bid side.
        ASK (str): Trade occurred on nearer to, or above, the ask side.
        MID (str): Trade occurred on the midpoint of the bid/ask.
        NO_SIDE (str): Trade occurred without a specific side.
    """

    BID = "B"
    ASK = "A"
    MID = "M"
    NO_SIDE = "N"


class Bias(Enum):
    """The side of the nbbo bid/ask that the trade occurred on."""
    BULLISH = "B"
    BEARISH = "R"
    NEUTRAL = "N"


class ExpirationDateT(datetime):
    """ExpirationDateT extends datetime with utilities for Option Expirations."""

    @classmethod
    def from_millis(cls, ms: int) -> "ExpirationDateT":
        """
        Create an ExpirationDate instance from milliseconds since epoch.

        Parameters
        ----------
        ms : int
            Milliseconds since epoch.

        Returns
        -------
        ExpirationDate
            An instance of ExpirationDate.

        Examples
        --------
        >>> ExpirationDateT.from_millis(1672531199000)
        ExpirationDateT(2023, 12, 31, 23, 59, 59)
        """
        return cls.utcfromtimestamp(ms / 1000.0)

    def to_millis(self) -> int:
        """
        Convert the ExpirationDate instance to milliseconds since epoch.

        Returns
        -------
        int
            Milliseconds since epoch.

        Examples
        --------
        >>> ed = ExpirationDateT(2023, 12, 31, 23, 59, 59)
        >>> ed.to_millis()
        1672531199000
        """
        return int(self.timestamp() * 1000)

    def to_str(self) -> str:
        """Format the expiration date as a string."""
        return self.strftime("%Y%m%d")

    @property
    def days_to_expiration(self) -> int:
        """Days between now and the option expiration date rounded
        down to the nearest whole number."""
        return (self - datetime.now()).days

    @property
    def years_to_expiration(self) -> float:
        """Years represented as a fraction between now and the
        option expiration date.
        Returns:
            float
                Time to expiration in years.

        Examples:
            >>> ed = ExpirationDateT(2024, 12, 31, 23, 59, 59)
            >>> ed.years_to_expiration
            1.0
        """

        delta = self - datetime.now()
        return delta.days / 365.25


@define
class OptionSymbolT:
    """Represents an option symbol in OCC standard format.

    for more information on the format see:
        - https://www.occ.com/education/trading-and-investing/trading-tools/options-on-common-stock/occ-options-on-common-stock-symbol-format
        - https://www.cboe.com/tradetools/occ/occ_options_on_common_stock_symbol_format.pdf
    """
    underlying: str = field(default="", kw_only=True)  # max 9 chars
    expiration_date: ExpirationDateT = field(kw_only=True)
    put_call: PutCall = field(kw_only=True)
    strike: PriceDecimalT = field(kw_only=True)
    symbol: str = field(default="", kw_only=True)

    def __init__(
        self,
        underlying: str | None = None,
        expiration_date: ExpirationDateT | None = None,
        put_call: PutCall | None = None,
        strike: PriceDecimalT | None = None,
        symbol: str | None = None,
    ) -> None:
        """Initialize the OptionSymbolT instance.

        Args:
            underlying: The underlying asset symbol.
            expiration_date: The expiration date of the option.
            put_call: The put or call of the option.
            strike: The strike price of the option.
            symbol: The OCC formatted option symbol.

        Raises:
            ValueError: If the symbol is not provided and not all individual components are provided.

        Returns:
            None

        Examples:
            >>> OptionSymbolT("AAPL", ExpirationDateT(2024, 12, 18), PutCall.CALL, PriceDecimalT("48.50"))
            >>> OptionSymbolT(symbol="AAPL241218C0048500")
        """

        if symbol:
            self.underlying = symbol[:6].strip()
            self.expiration_date = ExpirationDateT(int("20" + symbol[6:8]), int(symbol[8:10]), int(symbol[10:12]), 0, 0)
            self.put_call = PutCall(symbol[12])
            self.strike = PriceDecimalT(symbol[13:18] + "." + symbol[18:21])
        elif all(v is not None for v in [underlying, expiration_date, put_call, strike]):
            self.underlying = underlying
            self.expiration_date = expiration_date
            self.put_call = put_call
            self.strike = strike
            self.symbol = self.to_symbol()
        else:
            raise ValueError(
                "Either provide all individual components of the option symbol or a OCC formatted option string as the symbol."
            )

    @property
    def get_expiration_date_str(self) -> str:
        return self.expiration_date.strftime("%Y%m%d")

    def __str__(self) -> str:
        return self.to_symbol()

    def __repr__(self) -> str:
        return f"<OptionSymbolT(underlying={self.underlying}, expiration_date={self.expiration_date}, put_call={self.put_call}, strike={self.strike})>"

    def to_symbol(self) -> str:
        return f"{self.underlying: <6}{self.expiration_date.strftime('%y%m%d')}{self.put_call.value}{self.strike:05.03f}".strip()

    def to_dict(self) -> dict:
        """Convert the OptionSymbolT instance to a JSON-compatible dictionary.

        Args:
            None

        Returns:
            dict
                A dictionary containing the option symbol components:
                - underlying: str - The underlying asset symbol
                - expiration_date: str - The expiration date in YYYYMMDD format
                - put_call: str - The put/call indicator ('P' or 'C')
                - strike: float - The strike price

        Examples:
            ```python
            option = OptionSymbolT(
                'AAPL',
                ExpirationDateT(2024, 12, 18),
                PutCall.CALL,
                PriceDecimalT('48.50')
            )
            option.to_dict()
            # Returns:
            # {
            #     'underlying': 'AAPL',
            #     'expiration_date': '20241218',
            #     'put_call': 'C',
            #     'strike': 48.5,
            #     'symbol': 'AAPL241218C0048500'
            # }
            ```
        """
        return unstructure(self)

__all__ = [
    "BaseDecimalT",
    "PriceDecimalT",
    "FractionDecimalT",
    "TimestampT",
    "Side",
    "Bias",
    "ExpirationDateT",
    "OptionSymbolT",
]
