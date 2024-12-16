from typing import Dict


class DataProvider:
    """
    Immutable class to represent a data provider with its properties.

    Attributes:
        id : int
            The unique identifier for the data provider.
        symbol : str
            The symbol representing the data provider.
        type : str
            The type of the data provider (e.g., Vendor, Exchange).
        description : str
            A brief description of the data provider.
    """

    _id_to_symbol: Dict[int, str] = {
        1001: "DATABENTO",
        1002: "BLOOMBERG",
        1003: "THOMSON",
        1004: "REUTERS",
        1005: "FACTSET",
        1006: "REFINITIV",
        1007: "EIKON",
        1008: "REFINITIV",
        1009: "EIKON",
        1010: "POLYGON",
        1011: "IEX",
        1012: "CBOE",
        1013: "CME",
        1014: "NASDAQ",
        1015: "NYSE",
        1016: "ARCA",
        1017: "FMP",
        1018: "OPRA",
        1019: "ORATS",
        1020: "FINHUB",
        1021: "TRADESIGNALS",
        1022: "SEC.GOV",
        1023: "FDA.GOV",
        1024: "UNUSUALWHALES"
    }

    _symbol_to_id: Dict[str, int] = {v: k for k, v in _id_to_symbol.items()}

    _descriptions: Dict[str, str] = {
        "DATABENTO": "DATABENTO Third-Party Data",
        "BLOOMBERG": "Bloomberg Terminal",
        "THOMSON": "Thomson Reuters",
        "REUTERS": "Thomson Reuters",
        "FACTSET": "FactSet",
        "REFINITIV": "Refinitiv",
        "EIKON": "Thomson Reuters Eikon",
        "POLYGON": "Polygon.io Third-Party Data",
        "IEX": "Investors Exchange",
        "CBOE": "Chicago Board Options Exchange",
        "CME": "Chicago Mercantile Exchange",
        "NASDAQ": "National Association of Securities Dealers Automated Quotes",
        "NYSE": "New York Stock Exchange",
        "ARCA": "NYSE Arca",
        "FMP": "Financial Modeling Prep",
        "OPRA": "Options Price Reporting Authority",
        "ORATS": "Options Research and Trading System",
        "FINHUB": "FinHub Third-Party Data",
        "TRADESIGNALS": "Tradesignals Proprietary Data",
        "SEC.GOV": "Securities and Exchange Commission",
        "FDA.GOV": "Food and Drug Administration",
        "UNUSUALWHALES": "Market Data Vendor"
    }

    _types: Dict[str, str] = {
        "DATABENTO": "Vendor",
        "BLOOMBERG": "Vendor",
        "THOMSON": "Vendor",
        "REUTERS": "Vendor",
        "FACTSET": "Vendor",
        "REFINITIV": "Vendor",
        "EIKON": "Vendor",
        "POLYGON": "Vendor",
        "IEX": "Exchange",
        "CBOE": "Exchange",
        "CME": "Exchange",
        "NASDAQ": "Exchange",
        "NYSE": "Exchange",
        "ARCA": "Exchange",
        "FMP": "Vendor",
        "OPRA": "Vendor",
        "ORATS": "Vendor",
        "FINHUB": "Vendor",
        "TRADESIGNALS": "Vendor",
        "SEC.GOV": "",
        "FDA.GOV": "",
        "UNUSUALWHALES": "Vendor"
    }

    def __init__(self, id: int):
        if id not in self._id_to_symbol:
            raise ValueError("Invalid ID provided.")
        self._id = id
        self._symbol = self._id_to_symbol[id]

    @property
    def id(self) -> int:
        return self._id

    @property
    def symbol(self) -> str:
        return self._symbol

    @property
    def description(self) -> str:
        return self._descriptions[self._symbol]

    @property
    def type(self) -> str:
        return self._types[self._symbol]

    @classmethod
    def from_symbol(cls, symbol: str):
        """
        Create a DataProvider instance from a symbol.

        Args:
            symbol : str
                The symbol of the data provider.

        Returns:
            DataProvider
                An instance of DataProvider.

        Raises:
            ValueError
                If the symbol is invalid.
        """
        if symbol not in cls._symbol_to_id:
            raise ValueError("Invalid symbol provided.")
        return cls(cls._symbol_to_id[symbol])

    def __repr__(self):
        return f"DataProvider(id={self.id}, symbol='{self.symbol}', type='{self.type}', description='{self.description}')"
