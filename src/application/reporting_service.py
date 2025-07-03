"""
Contains the application service for processing portfolio data to generate reports.
This service acts as the calculation engine of the application.
"""

import pandas as pd
from src.domain.portfolio import Portfolio
from src.shared.financial_utils import calculate_inflation_period
from src.infrastructure.gateways import PPIGateway


class ReportingService:
    """
    Creates structured, enriched report DataFrames from a Portfolio object
    by applying business logic and fetching real-time market data.
    """

    def __init__(self, portfolio: Portfolio, ppi_gateway: PPIGateway):
        """
        Initializes the service with a Portfolio data object and a market data Gateway.

        Args:
            portfolio: The portfolio domain object with all required data.
            ppi_gateway: The gateway to fetch real-time market prices.
        """
        self.portfolio = portfolio
        self.ppi_gateway = ppi_gateway

    def _get_base_consolidated_assets(self) -> pd.DataFrame:
        """
        Performs the initial grouping and aggregation for non-option assets.

        Returns:
            A DataFrame with consolidated quantities, costs, and first purchase dates.
        """
        assets_df = self.portfolio.open_positions[
            ~self.portfolio.open_positions["asset_type"].isin(["OPCION"])
        ].copy()

        if assets_df.empty:
            return pd.DataFrame()

        agg_dict = {
            "quantity": "sum",
            "total_cost_ars": "sum",
            "total_cost_usd": "sum",
            "purchase_date": "min",
        }
        summary = assets_df.groupby("ticker").agg(agg_dict).reset_index()
        return summary

    def generate_open_positions_report(self) -> dict[str, pd.DataFrame]:
        """
        Generates a full report for open positions, including real-time performance.

        Returns:
            A dictionary containing two DataFrames: 'consolidated' for aggregated
            assets with performance, and 'options' for individual option lots.
        """
        # 1. Obtener los datos base agregados
        consolidated_df = self._get_base_consolidated_assets()

        if not consolidated_df.empty:
            # 2. Enriquecer con precios de mercado en tiempo real
            tickers = consolidated_df["ticker"].unique()
            price_cache = {t: self.ppi_gateway.get_current_price(t) for t in tickers}
            consolidated_df["current_price"] = consolidated_df["ticker"].map(
                price_cache
            )

            # 3. Calcular valores y rendimientos
            consolidated_df["current_value_ars"] = (
                consolidated_df["quantity"] * consolidated_df["current_price"]
            )

            # Evitar división por cero si el costo es cero
            consolidated_df["nominal_return_ars_pct"] = (
                (
                    consolidated_df["current_value_ars"]
                    / consolidated_df["total_cost_ars"]
                )
                - 1
            ) * 100

            today = pd.Timestamp.now()
            consolidated_df["inflation_arg"] = consolidated_df.apply(
                lambda row: calculate_inflation_period(
                    row["purchase_date"], today, self.portfolio.cpi_arg
                ),
                axis=1,
            )

            consolidated_df["real_return_ars_pct"] = (
                (
                    (1 + consolidated_df["nominal_return_ars_pct"] / 100)
                    / (1 + consolidated_df["inflation_arg"])
                )
                - 1
            ) * 100

            consolidated_df["age_days"] = (
                today - pd.to_datetime(consolidated_df["purchase_date"])
            ).dt.days

        # 4. Obtener posiciones de opciones sin modificar
        options_df = self.portfolio.open_positions[
            self.portfolio.open_positions["asset_type"] == "OPCION"
        ].copy()

        return {"consolidated": consolidated_df, "options": options_df}

    def generate_closed_trades_report(self) -> pd.DataFrame:
        """
        Calculates nominal and real performance for all closed trades.

        Returns:
            A DataFrame with performance metrics for each closed trade.
        """
        if self.portfolio.closed_trades.empty:
            return pd.DataFrame()

        df = self.portfolio.closed_trades.copy()

        # Calcular rendimientos nominales, manejando posible costo cero
        df["nominal_return_ars_pct"] = df.apply(
            lambda row: ((row["total_revenue_ars"] / row["total_cost_ars"]) - 1) * 100
            if row["total_cost_ars"] != 0
            else 0,
            axis=1,
        )
        df["nominal_return_usd_pct"] = df.apply(
            lambda row: ((row["total_revenue_usd"] / row["total_cost_usd"]) - 1) * 100
            if row["total_cost_usd"] != 0
            else 0,
            axis=1,
        )

        # Calcular inflación para el período de cada trade
        df["inflation_arg"] = df.apply(
            lambda row: calculate_inflation_period(
                row["buy_date"], row["sell_date"], self.portfolio.cpi_arg
            ),
            axis=1,
        )
        df["inflation_usa"] = df.apply(
            lambda row: calculate_inflation_period(
                row["buy_date"], row["sell_date"], self.portfolio.cpi_usa
            ),
            axis=1,
        )

        # Calcular rendimientos reales
        df["real_return_ars_pct"] = (
            ((1 + df["nominal_return_ars_pct"] / 100) / (1 + df["inflation_arg"])) - 1
        ) * 100
        df["real_return_usd_pct"] = (
            ((1 + df["nominal_return_usd_pct"] / 100) / (1 + df["inflation_usa"])) - 1
        ) * 100

        return df
