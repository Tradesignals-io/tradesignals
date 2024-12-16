"""
Module for handling price data.
"""

from decimal import Decimal, InvalidOperation, getcontext

FIXED_PRICE_SCALE = 6


class AvroDecimalT(Decimal):
    """Class for transporting fixed precision decimals."""

    def __init__(self, value: str, scale: int = FIXED_PRICE_SCALE) -> None:
        """Initialize the PriceDecimalT object.

        The class is designed to be used with price data but is flexible enough to
        be used with any decimal or float data type.  If you need to use a different
        scale, you can use the `set_scale` method, or when converting to and from
        bytes, you can pass in a different scale.

        Args:
            value : str
                The price value as a string.
            scale : int, optional
                The scale of the price.
        """
        super().__init__(value)
        self.scale = scale

    def __new__(cls, value: str, scale: int = FIXED_PRICE_SCALE) -> "AvroDecimalT":
        """Create a new AvroDecimalT object.

        Args:
            value : str
                The price value as a string.
            scale : int, optional
                The scale of the price.

        Returns:
            PriceDecimalT
                The new PriceDecimalT object.
        """
        self = super().__new__(cls, value)
        self.scale = scale
        return self

    def set_scale(self, scale: int = FIXED_PRICE_SCALE) -> "AvroDecimalT":
        """Set the scale of the AvroDecimalT object.

        Args:
            scale : int, optional
                The scale of the price.

        Returns:
            PriceDecimalT
                The PriceDecimalT object with the new scale.
        """
        self.scale = scale
        return self

    @classmethod
    def from_int(
        cls,
        price_int: int,
        scale: int = FIXED_PRICE_SCALE
    ) -> "AvroDecimalT":
        """Convert a Databento price integer to a AvroDecimalT object.

        Args:
            price_int : int
                The price as an integer.
            scale : int, optional
                The scale of the price.

        Returns:
            AvroDecimalT
                The price as a AvroDecimalT object.
        """
        instance = cls(str(price_int / 10**scale))
        instance.scale = scale
        return instance

    def to_int(self, scale: int = FIXED_PRICE_SCALE) -> int:
        """Convert the AvroDecimalT to an integer.

        Args:
            scale : int, optional
                The scale of the price.

        Returns:
            int
                The price as an integer scaled to 1e6.
        """
        return int(self * Decimal(10**scale))

    def to_bytes(self, scale: int = FIXED_PRICE_SCALE) -> bytes:
        """Convert AvroDecimalT value to avro bytes.

        Args:
            scale : int, optional
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
            return price_decimal.quantize(Decimal("1.000000")) \
                .to_eng_string().encode("utf-8")
        except (ValueError, ArithmeticError, InvalidOperation) as error:
            raise ValueError(f"Error converting price to bytes: {error}") from error


    @classmethod
    def from_bytes(
        cls,
        avro_bytes: bytes,
        scale: int = FIXED_PRICE_SCALE
    ) -> "AvroDecimalT":
        """Convert Avro bytes to AvroDecimalT.

        Args:
            avro_bytes : bytes
                The Avro bytes representing the price.
            scale : int, optional
                The scale of the price.

        Returns:
            AvroDecimalT
                The price as a AvroDecimalT object.

        Raises:
            ValueError
                If the Avro bytes cannot be converted to a valid AvroDecimalT.
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
            raise ValueError(
                f"Error converting Avro bytes `{avro_bytes}`: {error}"
            ) from error


__all__ = [
    "AvroDecimalT",
]
