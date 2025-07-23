import pandas as pd


class Portfolio:
    def __init__(
        self,
        open_positions: pd.DataFrame,
        closed_trades: pd.DataFrame,
        dolar_mep: pd.DataFrame,
        dolar_ccl: pd.DataFrame,
        cer_data: pd.DataFrame,
        cpi_usa: pd.DataFrame,
    ):
        self.open_positions = open_positions
        self.closed_trades = closed_trades
        self.dolar_mep = dolar_mep
        self.dolar_ccl = dolar_ccl
        self.cer_data = cer_data
        self.cpi_usa = cpi_usa
