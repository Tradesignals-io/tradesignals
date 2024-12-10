# flake8: noqa: E501
# pylint: disable=too-many-lines
"""
Custom types for Tradesignals.io market data.
"""

from enum import StrEnum


class PutCall(StrEnum):
    """
    ----------------------------------------------------------------
    Put Call string enumeration.
    ----------------------------------------------------------------

    This enumeration the various types of put and call options
    that can be used in the financial markets. Each type is represented by
    a single character string.

    Values
        P (str): put option, denoted by "P".
        C (str): call option, denoted by "C".
    """

    PUT = "P"
    CALL = "C"


class BookSide(StrEnum):
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


class TradeBias(StrEnum):
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

    BULLISH = "B"
    BEARISH = "R"
    NEUTRAL = "N"
    MID = "M"


__all__ = [
    "PutCall",
    "BookSide",
    "TradeBias",
]
