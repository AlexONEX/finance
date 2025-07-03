# src/domain/portfolio.py
"""
Defines the Portfolio entity, which holds the state of all financial data.
"""

import pandas as pd


class Portfolio:
    """
    Represents the portfolio state. It is initialized with pre-loaded data
    and does not handle file I/O or presentation logic.
    """

    def __init__(
        self,
        open_positions: pd.DataFrame,
        closed_trades: pd.DataFrame,
        dolar_mep: pd.DataFrame,
        dolar_ccl: pd.DataFrame,
        cpi_arg: pd.DataFrame,
        cpi_usa: pd.DataFrame,
    ):
        self.open_positions = open_positions
        self.closed_trades = closed_trades
        self.dolar_mep = dolar_mep
        self.dolar_ccl = dolar_ccl
        self.cpi_arg = cpi_arg
        self.cpi_usa = cpi_usa
