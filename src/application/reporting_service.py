# src/application/reporting_service.py
import pandas as pd
import logging
from src.domain.portfolio import Portfolio
from src.shared.financial_utils import calculate_inflation_period
from src.infrastructure.gateways.data912_connector import Data912APIConnector


class ReportingService:
    def __init__(self, portfolio: Portfolio):
        self.portfolio = portfolio
        self.api_connector = Data912APIConnector()
        self.price_cache = {}

    def _get_live_prices_by_type(self, asset_type: str):
        """
        Busca todos los precios en vivo para un tipo de activo y los guarda en caché.
        """
        # Si ya buscamos este tipo de activo, no lo volvemos a hacer.
        if asset_type in self.price_cache:
            return self.price_cache[asset_type]

        logging.info(f"Buscando precios en vivo para: {asset_type}...")

        # Mapeo de tipos de activo a funciones del conector
        fetcher_map = {
            "ACCION": self.api_connector.get_arg_stocks,
            "CEDEAR": self.api_connector.get_arg_cedears,
            "BONO": self.api_connector.get_arg_bonds,
            "LETRA": self.api_connector.get_arg_notes,
        }

        # Llama a la función correspondiente de la API
        fetch_function = fetcher_map.get(asset_type.upper())
        if not fetch_function:
            self.price_cache[asset_type] = {}  # Guardar caché vacío si no hay función
            return {}

        # Obtenemos los datos y los convertimos en un diccionario para búsqueda rápida
        live_data = fetch_function()
        prices = {item["t"]: item.get("c", 0) for item in live_data if "t" in item}

        # Guardamos el resultado en la caché para no volver a llamar a la API
        self.price_cache[asset_type] = prices
        return prices

    def _get_current_price(self, asset_type: str, ticker: str) -> float | None:
        """
        Obtiene el precio actual en vivo desde la API, usando una caché para ser eficiente.
        """
        if pd.isna(asset_type) or pd.isna(ticker):
            return None

        # Busca el diccionario de precios para el tipo de activo (usa la caché)
        price_dict = self._get_live_prices_by_type(asset_type)

        # Devuelve el precio para el ticker específico
        price = price_dict.get(ticker.upper())

        if price is None:
            logging.warning(f"No se encontró precio en vivo para el ticker: {ticker}")
            return None

        return float(price)

    def generate_open_positions_report(self) -> dict:
        """
        Calcula el rendimiento de las posiciones abiertas.
        """
        if self.portfolio.open_positions.empty:
            return {"consolidated": pd.DataFrame(), "options": pd.DataFrame()}

        self.price_cache = {}
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
