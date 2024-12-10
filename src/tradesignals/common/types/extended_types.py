"""Custom field types for TradeSignals."""

from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Union

from tradesignals.common.types.core import DecimalT, TimestampT

# Constants for precision and scale
PRICE_SCALE = 6
PRICE_PRECISION = 21
BIG_PRICE_SCALE = 10
BIG_PRICE_PRECISION = 38


class TradePriceT(DecimalT):
    """TradePriceT is a Decimal with a precision of 21 and a scale of 6."""

    def __init__(
        self,
        value: Union[Decimal, str],
        scale: int = PRICE_SCALE,
        currency_iso: str = "USD",
    ):
        """
        Initialize the TradePriceT.

        Parameters
        ----------
        value : Union[Decimal, str]
            The value to initialize the TradePriceT with.
        scale : int
            The scale to use for the TradePriceT.
        currency_iso : str
            The ISO code for the currency.
        """
        super().__init__(
            value,
            precision=PRICE_PRECISION,
            scale=scale
        )
        self.currency_iso = currency_iso


class TradeAmountT(DecimalT):
    """TradeAmountT is a Decimal with a precision of 38 and a scale of 10."""

    def __init__(
        self,
        value: Union[Decimal, str],
        scale: int = BIG_PRICE_SCALE,
        currency_iso: str = "USD",
    ):
        """
        Initialize the TradeAmountT.

        Parameters
        ----------
        value : Union[Decimal, str]
            The value to initialize the TradeAmountT with.
        scale : int
            The scale to use for the TradeAmountT.
        currency_iso : str
            The ISO code for the currency.
        """
        super().__init__(
            value,
            precision=BIG_PRICE_PRECISION,
            scale=scale,
        )
        self.currency_iso = currency_iso


class TradeTimestampT(TimestampT):
    """TradeTimestampT is a Decimal with a precision of 38 and a scale of 9."""

    def __init__(
        self,
        value: Union[int, str, datetime, date],
        tz: timezone = timezone.utc,
        resolution: str = 'ms',
    ):
        """
        Initialize the TradeTimestampT.

        Parameters
        ----------
        value : Union[int, str, datetime, date]
            The value to initialize the TradeTimestampT with.
        tz : timezone
            The timezone to use for the TradeTimestampT.
        resolution : str
            The resolution to use for the TradeTimestampT.
        """
        super().__init__(
            value,
            tz=tz,
            resolution=resolution,
        )


__all__ = [
    "TradePriceT",
    "TradeAmountT",
    "TradeTimestampT",
]
