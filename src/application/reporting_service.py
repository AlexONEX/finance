# src/application/reporting_service.py
import pandas as pd
import os
import logging
import config
from src.domain.portfolio import Portfolio
from src.shared.financial_utils import calculate_inflation_period


class ReportingService:
    def __init__(self, portfolio: Portfolio):
        self.portfolio = portfolio

    def _get_current_price(self, asset_type: str, ticker: str) -> float | None:
        """Lee el último precio del archivo CSV de datos históricos."""
        if pd.isna(asset_type) or pd.isna(ticker):
            return None

        asset_type_lower = asset_type.lower()
        file_path = os.path.join(
            config.DATA_DIR, f"historical_{asset_type_lower}_{ticker}.csv"
        )

        if not os.path.exists(file_path):
            logging.warning(
                f"No historical data file found for {ticker} at {file_path}"
            )
            return None

        try:
            hist_df = pd.read_csv(file_path)
            if not hist_df.empty:
                # Devuelve el último precio de la columna 'c' (close)
                return hist_df["c"].iloc[-1]
        except Exception as e:
            logging.error(f"Could not read or parse historical file for {ticker}: {e}")

        return None

    def generate_open_positions_report(self) -> dict:
        """
        Calcula el rendimiento de las posiciones abiertas.
        """
        if self.portfolio.open_positions.empty:
            return {"consolidated": pd.DataFrame(), "options": pd.DataFrame()}

        positions = self.portfolio.open_positions.copy()

        # 1. Calcular el precio y valor actual de cada posición
        positions["current_price"] = positions.apply(
            lambda row: self._get_current_price(row["asset_type"], row["ticker"]),
            axis=1,
        )

        # Filtrar filas donde no se pudo obtener el precio actual
        positions.dropna(subset=["current_price"], inplace=True)

        positions["current_value_ars"] = (
            positions["quantity"] * positions["current_price"]
        )

        # 2. Calcular rendimientos
        positions["nominal_return_ars"] = (
            positions["current_value_ars"] - positions["total_cost_ars"]
        )
        positions["nominal_return_ars_pct"] = (
            positions["nominal_return_ars"] / positions["total_cost_ars"]
        ) * 100

        # 3. Calcular antigüedad
        today = pd.Timestamp.now(tz="UTC").tz_convert(None)  # Fecha sin zona horaria
        positions["purchase_date"] = pd.to_datetime(
            positions["purchase_date"]
        ).dt.tz_localize(None)
        positions["age_days"] = (today - positions["purchase_date"]).dt.days

        # 4. Calcular rendimiento real (ajustado por inflación)
        positions["real_return_ars_pct"] = positions.apply(
            lambda row: (
                (1 + row["nominal_return_ars_pct"] / 100)
                / (
                    1
                    + calculate_inflation_period(
                        row["purchase_date"], today, self.portfolio.cpi_arg
                    )
                )
                - 1
            )
            * 100
            if pd.notna(row["purchase_date"])
            else None,
            axis=1,
        )

        # Separar por tipo de activo para la visualización
        consolidated_assets = positions[
            positions["asset_type"].isin(["ACCION", "CEDEAR", "RF", "BONO", "LETRA"])
        ]
        options = positions[positions["asset_type"] == "OPCION"]

        return {
            "consolidated": consolidated_assets,
            "options": options,
        }

    def generate_closed_trades_report(self) -> pd.DataFrame:
        """Calcula el rendimiento y otras métricas para las operaciones cerradas."""
        if self.portfolio.closed_trades.empty:
            return pd.DataFrame()

        report_df = self.portfolio.closed_trades.copy()

        # Calcular rendimientos nominales
        report_df["nominal_return_ars"] = (
            report_df["total_revenue_ars"] - report_df["total_cost_ars"]
        )
        report_df["nominal_return_usd"] = (
            report_df["total_revenue_usd"] - report_df["total_cost_usd"]
        )
        report_df["nominal_return_ars_pct"] = (
            report_df["nominal_return_ars"] / report_df["total_cost_ars"]
        ) * 100
        report_df["nominal_return_usd_pct"] = (
            report_df["nominal_return_usd"] / report_df["total_cost_usd"]
        ) * 100

        # Calcular rendimiento real
        report_df["real_return_ars_pct"] = report_df.apply(
            lambda row: (
                (1 + row["nominal_return_ars_pct"] / 100)
                / (
                    1
                    + calculate_inflation_period(
                        row["buy_date"], row["sell_date"], self.portfolio.cpi_arg
                    )
                )
                - 1
            )
            * 100
            if pd.notna(row["buy_date"])
            else None,
            axis=1,
        )
        report_df["real_return_usd_pct"] = report_df.apply(
            lambda row: (
                (1 + row["nominal_return_usd_pct"] / 100)
                / (
                    1
                    + calculate_inflation_period(
                        row["buy_date"], row["sell_date"], self.portfolio.cpi_usa
                    )
                )
                - 1
            )
            * 100
            if pd.notna(row["buy_date"])
            else None,
            axis=1,
        )

        return report_df
