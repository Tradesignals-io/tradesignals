"""Databento utilities."""
from decimal import Decimal, InvalidOperation, getcontext

VENUE_FROM_INT = {
    1: "GLBX", 2: "XNAS", 3: "XBOS", 4: "XPSX", 5: "BATS", 6: "BATY",
    7: "EDGA", 8: "EDGX", 9: "XNYS", 10: "XCIS", 11: "XASE", 12: "ARCX",
    13: "XCHI", 14: "IEXG", 15: "FINN", 16: "FINC", 17: "FINY", 18: "MEMX",
    19: "EPRL", 20: "AMXO", 21: "XBOX", 22: "XCBO", 23: "EMLD", 24: "EDGO",
    25: "GMNI", 26: "XISX", 27: "MCRY", 28: "XMIO", 29: "ARCO", 30: "OPRA",
    31: "MPRL", 32: "XNDQ", 33: "XBXO", 34: "C2OX", 35: "XPHL", 36: "BATO",
    37: "MXOP", 38: "IFEU", 39: "NDEX", 40: "DBEQ", 41: "SPHR"
}


def venue_id_to_name(id: int) -> str:
    return VENUE_FROM_INT[id]


DATASET_FROM_INT = {
    1: "GLBX.MDP3", 2: "XNAS.ITCH", 3: "XBOS.ITCH", 4: "XPSX.ITCH",
    5: "BATS.PITCH", 6: "BATY.PITCH", 7: "EDGA.PITCH", 8: "EDGX.PITCH",
    9: "XNYS.PILLAR", 10: "XCIS.PILLAR", 11: "XASE.PILLAR", 12: "XCHI.PILLAR",
    13: "XCIS.BBO", 14: "XCIS.TRADES", 15: "MEMX.MEMOIR", 16: "EPRL.DOM",
    17: "FINN.NLS", 18: "FINY.TRADES", 19: "OPRA.PILLAR", 20: "DBEQ.BASIC",
    21: "ARCX.PILLAR", 22: "IEXG.TOPS", 23: "DBEQ.PLUS", 24: "XNYS.BBO",
    25: "XNYS.TRADES", 26: "XNAS.QBBO", 27: "XNAS.NLS", 28: "IFEU.IMPACT",
    29: "NDEX.IMPACT"
}


def dataset_id_to_name(id: int) -> str:
    return DATASET_FROM_INT[id]


PUBLISHER_FROM_INT = {
    1: "GLBX.MDP3.GLBX", 2: "XNAS.ITCH.XNAS", 3: "XBOS.ITCH.XBOS",
    4: "XPSX.ITCH.XPSX", 5: "BATS.PITCH.BATS", 6: "BATY.PITCH.BATY",
    7: "EDGA.PITCH.EDGA", 8: "EDGX.PITCH.EDGX", 9: "XNYS.PILLAR.XNYS",
    10: "XCIS.PILLAR.XCIS", 11: "XASE.PILLAR.XASE", 12: "XCHI.PILLAR.XCHI",
    13: "XCIS.BBO.XCIS", 14: "XCIS.TRADES.XCIS", 15: "MEMX.MEMOIR.MEMX",
    16: "EPRL.DOM.EPRL", 17: "FINN.NLS.FINN", 18: "FINN.NLS.FINC",
    19: "FINY.TRADES.FINY", 20: "OPRA.PILLAR.AMXO", 21: "OPRA.PILLAR.XBOX",
    22: "OPRA.PILLAR.XCBO", 23: "OPRA.PILLAR.EMLD", 24: "OPRA.PILLAR.EDGO",
    25: "OPRA.PILLAR.GMNI", 26: "OPRA.PILLAR.XISX", 27: "OPRA.PILLAR.MCRY",
    28: "OPRA.PILLAR.XMIO", 29: "OPRA.PILLAR.ARCO", 30: "OPRA.PILLAR.OPRA",
    31: "OPRA.PILLAR.MPRL", 32: "OPRA.PILLAR.XNDQ", 33: "OPRA.PILLAR.XBXO",
    34: "OPRA.PILLAR.C2OX", 35: "OPRA.PILLAR.XPHL", 36: "OPRA.PILLAR.BATO",
    37: "OPRA.PILLAR.MXOP", 38: "IEXG.TOPS.IEXG", 39: "DBEQ.BASIC.XCHI",
    40: "DBEQ.BASIC.XCIS", 41: "DBEQ.BASIC.IEXG", 42: "DBEQ.BASIC.EPRL",
    43: "ARCX.PILLAR.ARCX", 44: "XNYS.BBO.XNYS", 45: "XNYS.TRADES.XNYS",
    46: "XNAS.QBBO.XNAS", 47: "XNAS.NLS.XNAS", 48: "DBEQ.PLUS.XCHI",
    49: "DBEQ.PLUS.XCIS", 50: "DBEQ.PLUS.IEXG", 51: "DBEQ.PLUS.EPRL",
    52: "DBEQ.PLUS.XNAS", 53: "DBEQ.PLUS.XNYS", 54: "DBEQ.PLUS.FINN",
    55: "DBEQ.PLUS.FINY", 56: "DBEQ.PLUS.FINC", 57: "IFEU.IMPACT.IFEU",
    58: "NDEX.IMPACT.NDEX", 59: "DBEQ.BASIC.DBEQ", 60: "DBEQ.PLUS.DBEQ",
    61: "OPRA.PILLAR.SPHR"
}


def publisher_id_to_name(id: int) -> str:
    return PUBLISHER_FROM_INT[id]

class PriceDecimalT(Decimal):
    """Class for handling fixed precision decimal numbers with to_int conversion."""

    def __init__(self, value: str, scale: int = 6) -> None:
        """
        Initialize the PriceDecimalT object.

        Args:
            value : str
                The price value as a string.
            scale : int
                The scale of the price.

        Returns:
            None
        """
        super().__init__(value)
        self.scale = scale

    def __new__(cls, value: str, scale: int = 6) -> 'PriceDecimalT':
        """
        Create a new PriceDecimalT object.

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

    def set_scale(self, scale: int = 6) -> 'PriceDecimalT':
        """
        Set the scale of the PriceDecimalT object.

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
    def from_int(cls, price_int: int, scale: int = 9) -> 'PriceDecimalT':
        """
        Convert a Databento price integer to a PriceDecimalT object.

        Args:
            price_int : int
                The price as an integer.
            scale : int
                The scale of the price.

        Returns:
            PriceDecimalT
                The price as a PriceDecimalT object.
        """
        return cls(str(price_int / 10 ** scale))

    def to_int(self, scale: int = 6) -> int:
        """
        Convert the PriceDecimalT to an integer with precision of 16 and scale of 6.

        Args:
            scale : int
                The scale of the price.

        Returns:
            int
                The price as an integer scaled to 1e6.
        """
        return int(self * Decimal(10 ** scale))

    def to_bytes(self, scale: int = 6) -> bytes:
        """
        Convert a Databento price integer to Avro bytes with precision of 18 and
        scale of 6.

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
        """
        try:
            getcontext().prec = 18
            price_decimal = self / Decimal(10 ** scale)
            price_decimal = price_decimal.quantize(Decimal('1.000000'))
            return price_decimal.to_eng_string().encode('utf-8')
        except (ValueError, ArithmeticError) as error:
            raise ValueError(
                f"Error converting price to bytes: {error}"
            ) from error

    @classmethod
    def from_bytes(cls, avro_bytes: bytes, scale: int = 6) -> 'PriceDecimalT':
        """
        Convert Avro bytes back to a PriceDecimalT with precision of 16 and scale of 6.

        Args:
            avro_bytes : bytes
                The Avro bytes representing the price.
            scale : int
                The scale of the price.

        Returns:
            PriceDecimalT
                The price as a PriceDecimalT object with precision of 16 and scale of 6.

        Raises:
            ValueError
                If the Avro bytes cannot be converted to a valid PriceDecimalT.
        """
        try:
            price_str = avro_bytes.decode('utf-8')
            price_decimal = Decimal(price_str).quantize(Decimal('1.000000'))
            return cls(str(price_decimal))
        except (ValueError, ArithmeticError, InvalidOperation) as error:
            raise ValueError(
                f"Error converting Avro bytes `{avro_bytes}`: {error}"
            ) from error

__all__ = [
    "VENUE_FROM_INT",
    "DATASET_FROM_INT",
    "PUBLISHER_FROM_INT"
]
