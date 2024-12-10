# flake8: noqa
from attrs import define
from cattrs import structure, unstructure
from datetime import datetime, timedelta
from decimal import Decimal
from typing import TypeVar

from tradesignals.common.types.core import PriceT, PutCall


class ExpirationDateT(datetime):
    """
    ExpirationDate extends the native datetime class with additional methods
    for converting to and from milliseconds.
    """

    @classmethod
    def from_millis(cls, ms: int) -> 'ExpirationDate':
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
        >>> ExpirationDate.from_milliseconds(1672531199000)
        ExpirationDate(2023, 12, 31, 23, 59, 59)
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
        >>> ed = ExpirationDate(2023, 12, 31, 23, 59, 59)
        >>> ed.to_milliseconds()
        1672531199000
        """
        return int(self.timestamp() * 1000)

    def to_str(self) -> str:
        return self.strftime("%Y%m%d")

    @property
    def days_to_expiration(self) -> int:
        return (self - datetime.now()).days

    @property
    def years_to_expiration(self) -> float:
        """
        Calculate the time to expiration in years.

        Returns
        -------
        float
            Time to expiration in years.

        Examples
        --------
        >>> ed = ExpirationDate(2024, 12, 31, 23, 59, 59)
        >>> ed.time_to_expiration
        1.0
        """
        delta = self - datetime.now()
        return delta.days / 365.25


@define
class OptionSymbolT:
    """
    Represents an option symbol in OCC standard format.

    for more information on the format see:
        - https://www.occ.com/education/trading-and-investing/trading-tools/options-on-common-stock/occ-options-on-common-stock-symbol-format
        - https://www.cboe.com/tradetools/occ/occ_options_on_common_stock_symbol_format.pdf
    """
    underlying: str
    expiration_date: ExpirationDateT
    put_call: PutCall
    strike: PriceT
    symbol: str

    def __init__(
        self,
        underlying: str = None,
        expiration_date: ExpirationDateT = None,
        put_call: PutCall = None,
        strike: PriceT = None,
        symbol: str = None,
    ):
        """
        Initialize an OptionSymbolT instance either from individual
        components or a single formatted string.

        Parameters
        ----------
        underlying : str, optional
            The ticker symbol of the stock (e.g., 'AAL', 'F').
        expiration_date : ExpirationDateT, optional
            The expiration date of the option.
        put_call : str, optional
            'C' for Call option, 'P' for Put option.
        strike : float, optional
            The strike price of the option (e.g., 48.50).
        symbol : str, optional
            A single formatted string representing the option symbol.

        Raises
        ------
        ValueError
            If neither individual components nor a valid option string is provided.

        Examples
        --------
        >>> option = OptionSymbolT('AAPL', ExpirationDate(2024, 12, 18), 'C', PriceT('48.50'))
        >>> option = OptionSymbolT(symbol='AAPL 241218C048500')
        """
        if symbol:
            self.underlying = symbol[:6].strip()
            self.expiration_date = ExpirationDateT(int("20" + symbol[6:8]), int(symbol[8:10]), int(symbol[10:12]), 0, 0)
            self.put_call = symbol[12]
            self.strike = PriceT(symbol[13:18] + "." + symbol[18:21])
        elif all(v is not None for v in [underlying, expiration_date, put_call, strike]):
            self.underlying = underlying
            self.expiration_date = expiration_date
            self.put_call = put_call
            self.strike = strike
            self.symbol = self.to_symbol()
        else:
            raise ValueError("Either provide all individual components of the option symbol or a OCC formatted option string as the symbol.")

    @property
    def get_expiration_date_str(self) -> str:
        return self.expiration_date.strftime("%Y%m%d")

    def __str__(self) -> str:
        return self.to_symbol()

    def __repr__(self) -> str:
        return f"<OptionSymbolT(underlying={self.underlying}, expiration_date={self.expiration_date}, put_call={self.put_call}, strike={self.strike})>"

    def to_symbol(self) -> str:
        return f"{self.underlying: <6}{self.expiration_date.strftime('%y%m%d')}{self.put_call}{self.strike:05.03f}".strip()

    def to_dict(self) -> dict:
        """
        Convert the OptionSymbolT instance to a JSON-compatible dictionary.

        Returns
        -------
        dict
            A dictionary representation of the option symbol with keys:
            'underlying',
            'expiration_date',
            'put_call',
            'strike'

        Examples
        --------
        >>> option = OptionSymbolT('AAPL', ExpirationDate(2024, 12, 18), 'C', PriceT('48.50'))
        >>> option.to_dict()
        {'underlying': 'AAPL', 'expiration_date': '20241218', 'put_call': 'C', 'strike': 48.5}
        """
        return unstructure(self)

# Example usage
option = OptionSymbolT('F', ExpirationDate(2024, 12, 18), 'C', PriceT('48.50'))
option_json = option.to_dict()
print(option_json)  # Output: {'underlying': 'F', 'expiration_date': '20241218', 'put_call': 'C', 'strike': 48.5}

option_from_str = OptionSymbolT(symbol='F     241218C048500')
option_from_str_json = option_from_str.to_dict()
print(option_from_str_json)  # Output: {'underlying': 'F', 'expiration_date': '20241218', 'put_call': 'C', 'strike': 48.5}

import unittest
from datetime import datetime

class TestOptionSymbolT(unittest.TestCase):
    def setUp(self):
        self.underlying = 'AAPL'
        self.expiration_date = datetime(2024, 12, 18)
        self.put_call = 'C'
        self.strike = PriceT('48.50')
        self.option = OptionSymbolT(self.underlying, self.expiration_date, self.put_call, self.strike)

    def test_to_symbol(self):
        expected_symbol = 'AAPL  241218C048.500'
        self.assertEqual(self.option.to_symbol(), expected_symbol)

    def test_to_dict(self):
        expected_dict = {
            'underlying': 'AAPL',
            'expiration_date': '20241218',
            'put_call': 'C',
            'strike': 48.5
        }
        self.assertEqual(self.option.to_dict(), expected_dict)

    def test_str(self):
        expected_str = 'AAPL  241218C048.500'
        self.assertEqual(str(self.option), expected_str)

    def test_repr(self):
        expected_repr = "<OptionSymbolT(underlying=AAPL, expiration_date=2024-12-18 00:00:00, put_call=C, strike=48.5)>"
        self.assertEqual(repr(self.option), expected_repr)

    def test_from_symbol(self):
        option_from_str = OptionSymbolT(symbol='AAPL  241218C048500')
        self.assertEqual(option_from_str.to_dict(), self.option.to_dict())

    def test_invalid_initialization(self):
        with self.assertRaises(ValueError):
            OptionSymbolT('AAPL', self.expiration_date, 'C')

if __name__ == '__main__':
    unittest.main()
