# flake8: noqa: E501
"""
---------------------------------------------------------------------
# Option Calculator Option price, volatility, and other computations.
---------------------------------------------------------------------

## Calculations included:
    - Black76 theo option price.
    - Daily theo P&L
    - Point-in-time theo P&L
    - Delta
    - Gamma
    - Theta
    - Vega
    - Rho
    - Implied Volatility
    - Volatility Surface
    - Volatility Skew
"""
import numpy as np
from scipy.stats import norm

from tradesignals.common.types import DecimalT

RISK_FREE_INTEREST_RATE = 0.05

# @todo: convert method to use and return DecimalT

def black_76(
    S: float,  # spot price
    K: float,  # strike price
    T: float,  # time to expiration expressed as years fraction
    sigma: float,  # implied annualized volatility
    r: float = RISK_FREE_INTEREST_RATE,  # risk-free interest rate
    is_call: bool = True,  # denotes a put or call option
) -> DecimalT:
    """
    Calculate an option price from the Black-76 model.

    Parameters
    ----------
    S : float
        Current underlying price.
    K : float
        Option strike price.
    T : float
        Time to expiration in years.
    sigma : float
        Implied volatility, annualized.
    r : float, default 0.05
        The risk free interest rate, annualized.
    is_call : bool, default True
        Flag to indicate the option is a call or put.

    Returns
    -------
    float

    """
    d1 = (np.log(S / K) + (sigma**2 / 2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    discount_factor = np.exp(-r * T)

    cp = 1 if is_call else -1
    return discount_factor * cp * (S * norm.cdf(cp * d1) - K * norm.cdf(cp * d2))

__all__ = ["black_76"]
