"""
Price Types
-----------

PriceT is a Decimal with a precision of 21 and a scale of 6.
BigPriceT is a Decimal with a precision of 38 and a scale of 10.

PriceT and BigPriceT are used to represent prices in financial data and have
methods for conversion to and from avro bytes.

"""
import logging
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Union

ROUNDING_MODE = ROUND_HALF_UP

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.FileHandler("decimal.log"))

class DecimalT:
    """Create a DecimalT object with the specified precision and scale."""

    def __init__(
        self,
        value: Decimal,
        precision: int,
        scale: int
    ):
        """
        Initialize the DecimalT object.

        Parameters
        ----------
        value : Decimal
            The value to initialize the DecimalT with.
        precision : int
            The precision to use for the DecimalT.
        scale : int
            The scale to use for the DecimalT.

        Raises
        ------
        ValueError
            If the value is not a Decimal.
        """
        self.scale = scale
        self.precision = precision
        if not isinstance(value, Decimal):
            raise ValueError("Value must be a Decimal.")
        self.value = value

    def to_decimal(self) -> Decimal:
        """
        Convert the stored value to a Decimal.

        Returns
        -------
        Decimal
            The decimal representation of the stored value.
        """
        return self.value

    def to_bytes(self) -> bytes:
        """
        Convert the stored decimal value to Avro-compatible bytes.

        Returns
        -------
        bytes
            A bytes object containing the Avro-encoded decimal.
        """
        scaled_value_int = int(self.to_decimal() * (10**self.scale))
        byte_length = (scaled_value_int.bit_length() + 8) // 8
        return scaled_value_int.to_bytes(byte_length, byteorder="big", signed=True)

    @classmethod
    def from_bytes(
        cls,
        value: bytes,
        precision: int,
        scale: int
    ) -> 'DecimalT':
        """
        Convert Avro-compatible bytes back to a DecimalT.

        Parameters
        ----------
        value : bytes
            The Avro-encoded bytes.
        precision : int
            The precision to use for the DecimalT.
        scale : int
            The scale to use for the DecimalT.

        Returns
        -------
        DecimalT
            A DecimalT object.
        """
        unscaled_value = int.from_bytes(value, byteorder="big", signed=True)
        return cls(
            Decimal(unscaled_value) / (10**scale),
            precision=precision,
            scale=scale
        )

    @classmethod
    def from_int(
        cls,
        value: int,
        precision: int,
        scale: int
    ) -> 'DecimalT':
        """
        Create a DecimalT from an integer.

        Parameters
        ----------
        value : int
            The integer to create the DecimalT from.
        precision : int
            The precision to use for the DecimalT.
        scale : int
            The scale to use for the DecimalT.

        Returns
        -------
        DecimalT
            A DecimalT object.
        """
        return cls(
            Decimal(value) / (10**scale),
            precision=precision,
            scale=scale
        )

    @classmethod
    def from_float(
        cls,
        value: float,
        precision: int,
        scale: int
    ) -> 'DecimalT':
        """
        Create a DecimalT from a float.

        Parameters
        ----------
        value : float
            The float to create the DecimalT from.
        precision : int
            The precision to use for the DecimalT.
        scale : int
            The scale to use for the DecimalT.

        Returns
        -------
        DecimalT
            A DecimalT object.
        """
        return cls(
            Decimal(value),
            precision=precision,
            scale=scale
        )

    @classmethod
    def from_str(
        cls,
        value: str,
        precision: int,
        scale: int
    ) -> 'DecimalT':
        """
        Create a DecimalT from a string.

        Parameters
        ----------
        value : str
            The string to create the DecimalT from.
        precision : int
            The precision to use for the DecimalT.
        scale : int
            The scale to use for the DecimalT.

        Returns
        -------
        DecimalT
            A DecimalT object.

        Raises
        ------
        ValueError
            If the string cannot be converted to a Decimal.
        """
        logger.info(f"Converting string to Decimal: {value}")
        print(f"Converting string to Decimal: {value}")
        print("error!")
        try:
            decimal_value = Decimal(value)
        except InvalidOperation:
            raise ValueError(f"Invalid string for Decimal conversion: {value}")

        return cls(
            decimal_value,
            precision=precision,
            scale=scale
        )

    def __str__(self) -> str:
        """
        Convert the stored value to a string.

        Returns
        -------
        str
            The string representation of the stored value.
        """
        return str(self.value)

    def __repr__(self) -> str:
        """
        Return a string representation of the DecimalT object.

        Returns
        -------
        str
            The string representation of the DecimalT object.
        """
        return f"DecimalT({self.value})"

    # Arithmetic operations
    def __add__(self, other: 'DecimalT') -> 'DecimalT':
        """
        Add two DecimalT objects.

        Parameters
        ----------
        other : DecimalT
            The other DecimalT to add.

        Returns
        -------
        DecimalT
            The result of the addition.
        """
        return DecimalT(self.value + other.to_decimal(), self.precision, self.scale)

    def __sub__(self, other: 'DecimalT') -> 'DecimalT':
        """
        Subtract a DecimalT from another.

        Parameters
        ----------
        other : DecimalT
            The other DecimalT to subtract.

        Returns
        -------
        DecimalT
            The result of the subtraction.
        """
        return DecimalT(self.value - other.to_decimal(), self.precision, self.scale)

    def __mul__(self, other: 'DecimalT') -> 'DecimalT':
        """
        Multiply two DecimalT objects.

        Parameters
        ----------
        other : DecimalT
            The other DecimalT to multiply.

        Returns
        -------
        DecimalT
            The result of the multiplication.
        """
        return DecimalT(self.value * other.to_decimal(), self.precision, self.scale)

    def __truediv__(self, other: 'DecimalT') -> 'DecimalT':
        """
        Divide a DecimalT by another.

        Parameters
        ----------
        other : DecimalT
            The other DecimalT to divide by.

        Returns
        -------
        DecimalT
            The result of the division.
        """
        return DecimalT(self.value / other.to_decimal(), self.precision, self.scale)

    # Comparison operations
    def __eq__(self, other: 'DecimalT') -> bool:
        """
        Check if two DecimalT objects are equal.

        Parameters
        ----------
        other : DecimalT
            The other DecimalT to compare.

        Returns
        -------
        bool
            True if equal, False otherwise.
        """
        return self.value == other.to_decimal()

    def __ne__(self, other: 'DecimalT') -> bool:
        """
        Check if two DecimalT objects are not equal.

        Parameters
        ----------
        other : DecimalT
            The other DecimalT to compare.

        Returns
        -------
        bool
            True if not equal, False otherwise.
        """
        return self.value != other.to_decimal()

    def __lt__(self, other: 'DecimalT') -> bool:
        """
        Check if this DecimalT is less than another.

        Parameters
        ----------
        other : DecimalT
            The other DecimalT to compare.

        Returns
        -------
        bool
            True if less, False otherwise.
        """
        return self.value < other.to_decimal()

    def __le__(self, other: 'DecimalT') -> bool:
        """
        Check if this DecimalT is less than or equal to another.

        Parameters
        ----------
        other : DecimalT
            The other DecimalT to compare.

        Returns
        -------
        bool
            True if less or equal, False otherwise.
        """
        return self.value <= other.to_decimal()

    def __gt__(self, other: 'DecimalT') -> bool:
        """
        Check if this DecimalT is greater than another.

        Parameters
        ----------
        other : DecimalT
            The other DecimalT to compare.

        Returns
        -------
        bool
            True if greater, False otherwise.
        """
        return self.value > other.to_decimal()

    def __ge__(self, other: 'DecimalT') -> bool:
        """
        Check if this DecimalT is greater than or equal to another.

        Parameters
        ----------
        other : DecimalT
            The other DecimalT to compare.

        Returns
        -------
        bool
            True if greater or equal, False otherwise.
        """
        return self.value >= other.to_decimal()

    def __eq__(self, other: 'DecimalT') -> bool:
        """
        Check if this DecimalT is equal to another.

        Parameters
        ----------
        other : DecimalT
            The other DecimalT to compare.

        Returns
        -------
        bool
            True if equal, False otherwise.
        """
        return self.value == other.to_decimal()

    def __ne__(self, other: 'DecimalT') -> bool:
        """
        Check if this DecimalT is not equal to another.

        Parameters
        ----------
        other : DecimalT
            The other DecimalT to compare.

        Returns
        -------
        bool
            True if not equal, False otherwise.
        """
        return self.value != other.to_decimal()

    def __neg__(self) -> 'DecimalT':
        """
        Negate this DecimalT.

        Returns
        -------
        DecimalT
            The negated DecimalT.
        """
        return DecimalT(-self.value, self.precision, self.scale)

    def __abs__(self) -> 'DecimalT':
        """
        Get the absolute value of this DecimalT.

        Returns
        -------
        DecimalT
            The absolute value of the DecimalT.
        """
        return DecimalT(abs(self.value), self.precision, self.scale)

    def __truediv__(self, other: 'DecimalT') -> 'DecimalT':
        """
        Divide this DecimalT by another.

        Parameters
        ----------
        other : DecimalT
            The other DecimalT to divide by.

        Returns
        -------
        DecimalT
            The result of the division.
        """
        return DecimalT(self.value / other.to_decimal(), self.precision, self.scale)

    def __floordiv__(self, other: 'DecimalT') -> 'DecimalT':
        """
        Perform floor division on this DecimalT by another.

        Parameters
        ----------
        other : DecimalT
            The other DecimalT to divide by.

        Returns
        -------
        DecimalT
            The result of the floor division.
        """
        return DecimalT(self.value // other.to_decimal(), self.precision, self.scale)

    def __mod__(self, other: 'DecimalT') -> 'DecimalT':
        """
        Get the modulus of this DecimalT by another.

        Parameters
        ----------
        other : DecimalT
            The other DecimalT to divide by.

        Returns
        -------
        DecimalT
            The result of the modulus operation.
        """
        return DecimalT(self.value % other.to_decimal(), self.precision, self.scale)

    def __pow__(self, other: 'DecimalT') -> 'DecimalT':
        """
        Raise this DecimalT to the power of another.

        Parameters
        ----------
        other : DecimalT
            The other DecimalT to use as the exponent.

        Returns
        -------
        DecimalT
            The result of the exponentiation.
        """
        return DecimalT(self.value ** other.to_decimal(), self.precision, self.scale)

    def __radd__(self, other: 'DecimalT') -> 'DecimalT':
        """
        Add another DecimalT to this DecimalT (reversed operands).

        Parameters
        ----------
        other : DecimalT
            The other DecimalT to add.

        Returns
        -------
        DecimalT
            The result of the addition.
        """
        return self.__add__(other)

    def __rsub__(self, other: 'DecimalT') -> 'DecimalT':
        """
        Subtract this DecimalT from another (reversed operands).

        Parameters
        ----------
        other : DecimalT
            The other DecimalT to subtract from.

        Returns
        -------
        DecimalT
            The result of the subtraction.
        """
        return DecimalT(other.to_decimal() - self.value, self.precision, self.scale)

    def __rmul__(self, other: 'DecimalT') -> 'DecimalT':
        """
        Multiply another DecimalT by this DecimalT (reversed operands).

        Parameters
        ----------
        other : DecimalT
            The other DecimalT to multiply.

        Returns
        -------
        DecimalT
            The result of the multiplication.
        """
        return self.__mul__(other)

    def __rtruediv__(self, other: 'DecimalT') -> 'DecimalT':
        """
        Divide another DecimalT by this DecimalT (reversed operands).

        Parameters
        ----------
        other : DecimalT
            The other DecimalT to divide.

        Returns
        -------
        DecimalT
            The result of the division.
        """
        return DecimalT(other.to_decimal() / self.value, self.precision, self.scale)

    def __rfloordiv__(self, other: 'DecimalT') -> 'DecimalT':
        """
        Perform floor division on another DecimalT by this DecimalT (reversed operands).

        Parameters
        ----------
        other : DecimalT
            The other DecimalT to divide.

        Returns
        -------
        DecimalT
            The result of the floor division.
        """
        return DecimalT(other.to_decimal() // self.value, self.precision, self.scale)

    def __rmod__(self, other: 'DecimalT') -> 'DecimalT':
        """
        Get the modulus of another DecimalT by this DecimalT (reversed operands).

        Parameters
        ----------
        other : DecimalT
            The other DecimalT to divide.

        Returns
        -------
        DecimalT
            The result of the modulus operation.
        """
        return DecimalT(other.to_decimal() % self.value, self.precision, self.scale)

    def __rpow__(self, other: 'DecimalT') -> 'DecimalT':
        """
        Raise another DecimalT to the power of this DecimalT (reversed operands).

        Parameters
        ----------
        other : DecimalT
            The other DecimalT to use as the base.

        Returns
        -------
        DecimalT
            The result of the exponentiation.
        """
        return DecimalT(other.to_decimal() ** self.value, self.precision, self.scale)


__all__ = [
    "DecimalT"
]
