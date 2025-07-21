import os
import pandas as pd
import config
import logging
from src.domain.portfolio import Portfolio


class PortfolioRepository:
    """Manages loading and saving all portfolio data."""

    def _load_csv(self, file_path: str, parse_dates: list = None) -> pd.DataFrame:
        if not (os.path.exists(file_path) and os.path.getsize(file_path) > 0):
            return pd.DataFrame()
        try:
            df = pd.read_csv(file_path)
            if parse_dates:
                existing_date_cols = [col for col in parse_dates if col in df.columns]
                for col in existing_date_cols:
                    df[col] = pd.to_datetime(df[col], errors="coerce")
            return df
        except Exception as e:
            logging.error(f"Could not load or parse CSV file at {file_path}: {e}")
            return pd.DataFrame()

    def load_full_portfolio(self) -> Portfolio:
        """Loads all data files and instantiates the Portfolio domain object."""
        open_positions = self._load_csv(
            config.OPEN_POSITIONS_FILE, ["purchase_date", "expiration_date"]
        )
        closed_trades = self._load_csv(
            config.CLOSED_TRADES_FILE, ["buy_date", "sell_date"]
        )
        dolar_mep = self._load_csv(config.DOLAR_MEP_FILE, ["date"])
        dolar_ccl = self._load_csv(config.DOLAR_CCL_FILE, ["date"])
        cer_data = self._load_csv(config.CER_FILE, ["date"])
        cpi_usa = self._load_csv(config.CPI_USA_FILE, ["date"])

        return Portfolio(
            open_positions, closed_trades, dolar_mep, dolar_ccl, cer_data, cpi_usa
        )

    def save_open_positions(self, open_positions_df: pd.DataFrame):
        """Saves the open positions DataFrame to its CSV file."""
        open_positions_df.to_csv(config.OPEN_POSITIONS_FILE, index=False)

    def save_closed_trades(self, closed_trades_df: pd.DataFrame):
        """Saves the closed trades DataFrame to its CSV file."""
        closed_trades_df.to_csv(config.CLOSED_TRADES_FILE, index=False)
