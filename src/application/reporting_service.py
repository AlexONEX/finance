import pandas as pd
import logging
import config
from src.domain.portfolio import Portfolio
from src.shared.financial_utils import calculate_inflation_period
from src.infrastructure.gateways.data912_connector import Data912APIConnector


class ReportingService:
    def __init__(self, portfolio: Portfolio):
        self.portfolio = portfolio
        self.api_connector = Data912APIConnector()
        self.price_cache = {}

    def _get_live_prices_by_type(self, asset_type: str):
        asset_type = asset_type.upper()
        if asset_type == "OPCION":
            return {}
        fixed_income_types = ["RF", "BONO", "LETRA", "ON"]
        if asset_type in fixed_income_types:
            if "fixed_income" in self.price_cache:
                return self.price_cache["fixed_income"]
            logging.info(
                "Buscando precios en vivo para toda la Renta Fija (Bonos, Letras, ONs)..."
            )
            all_prices = {}
            fetch_functions = [
                self.api_connector.get_arg_bonds,
                self.api_connector.get_arg_notes,
                self.api_connector.get_arg_corporate_debt,
            ]
            for fetch in fetch_functions:
                live_data = fetch()
                if isinstance(live_data, list):
                    prices = {
                        item["symbol"]: item.get("c", 0)
                        for item in live_data
                        if "symbol" in item
                    }
                    all_prices.update(prices)
            self.price_cache["fixed_income"] = all_prices
            return all_prices
        if asset_type in self.price_cache:
            return self.price_cache[asset_type]
        logging.info(f"Buscando precios en vivo para: {asset_type}...")
        fetcher_map = {
            "ACCION": self.api_connector.get_arg_stocks,
            "CEDEAR": self.api_connector.get_arg_cedears,
        }
        fetch_function = fetcher_map.get(asset_type)
        if not fetch_function:
            self.price_cache[asset_type] = {}
            return {}
        live_data = fetch_function()
        if not isinstance(live_data, list):
            self.price_cache[asset_type] = {}
            return {}
        prices = {
            item["symbol"]: item.get("c", 0) for item in live_data if "symbol" in item
        }
        self.price_cache[asset_type] = prices
        return prices

    def _get_current_price(self, asset_type: str, ticker: str) -> float | None:
        if pd.isna(asset_type) or pd.isna(ticker):
            return None
        price_dict = self._get_live_prices_by_type(asset_type)
        price = price_dict.get(ticker.upper())

        if price is None:
            logging.warning(f"No se encontró precio en vivo para el ticker: {ticker}")
            return None

        if asset_type.upper() in ["RF", "BONO", "LETRA", "ON"]:
            return float(price) / config.BOND_PRICE_DIVISOR

        return float(price)

    def generate_open_positions_report(self) -> dict:
        if self.portfolio.open_positions.empty:
            return {"consolidated": pd.DataFrame(), "options": pd.DataFrame()}

        self.price_cache = {}
        positions = self.portfolio.open_positions.copy()
        options_positions = positions[positions["asset_type"] == "OPCION"]

        # Trabajar solo con posiciones que no son opciones
        positions = positions[positions["asset_type"] != "OPCION"].copy()

        if positions.empty:
            return {"consolidated": pd.DataFrame(), "options": options_positions}

        consolidated = (
            positions.groupby("ticker")
            .apply(
                lambda x: pd.Series(
                    {
                        "quantity": x["quantity"].sum(),
                        "total_cost_ars": x["total_cost_ars"].sum(),
                        "total_cost_usd": x["total_cost_usd"].sum(),
                        "asset_type": x["asset_type"].iloc[
                            0
                        ],  # Tomar el tipo del primer lote
                        "first_purchase_date": x["purchase_date"].min(),
                    }
                )
            )
            .reset_index()
        )

        # Calcular precio de compra promedio ponderado
        consolidated["buy_price_ars"] = (
            consolidated["total_cost_ars"] / consolidated["quantity"]
        )

        # Obtener precios actuales para la cartera consolidada
        consolidated["current_price"] = consolidated.apply(
            lambda row: self._get_current_price(row["asset_type"], row["ticker"]),
            axis=1,
        )
        consolidated.dropna(subset=["current_price"], inplace=True)

        # Calcular rendimientos sobre la base consolidada
        consolidated["nominal_return_ars_pct"] = (
            consolidated["current_price"] / consolidated["buy_price_ars"] - 1
        ) * 100

        today = pd.Timestamp.now().normalize()
        consolidated["age_days"] = (
            today - pd.to_datetime(consolidated["first_purchase_date"])
        ).dt.days

        consolidated["real_return_ars_pct"] = consolidated.apply(
            lambda row: (
                (1 + row["nominal_return_ars_pct"] / 100)
                / (
                    1
                    + calculate_inflation_period(
                        row["first_purchase_date"], today, self.portfolio.cer_data
                    )
                )
                - 1
            )
            * 100,
            axis=1,
        )

        return {
            "consolidated": consolidated,
            "options": options_positions,
        }

    def generate_closed_trades_report(self) -> pd.DataFrame:
        if self.portfolio.closed_trades.empty:
            return pd.DataFrame()
        report_df = self.portfolio.closed_trades.copy()
        report_df["total_cost_ars"] = report_df["total_cost_ars"].replace(0, pd.NA)
        report_df["total_cost_usd"] = report_df["total_cost_usd"].replace(0, pd.NA)
        report_df["nominal_return_ars_pct"] = (
            (report_df["total_revenue_ars"] - report_df["total_cost_ars"])
            / report_df["total_cost_ars"]
        ) * 100
        report_df["nominal_return_usd_pct"] = (
            (report_df["total_revenue_usd"] - report_df["total_cost_usd"])
            / report_df["total_cost_usd"]
        ) * 100
        report_df["real_return_ars_pct"] = report_df.apply(
            lambda row: (
                (1 + row["nominal_return_ars_pct"] / 100)
                / (
                    1
                    + calculate_inflation_period(
                        row["buy_date"],
                        row["sell_date"],
                        self.portfolio.cer_data,
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
                        row["buy_date"],
                        row["sell_date"],
                        self.portfolio.cpi_usa,
                    )
                )
                - 1
            )
            * 100
            if pd.notna(row["buy_date"])
            else None,
            axis=1,
        )

        def weighted_avg(group, avg_col, weight_col):
            d = group[avg_col]
            w = group[weight_col]
            try:
                return (d * w).sum() / w.sum()
            except ZeroDivisionError:
                return 0

        consolidated_df = (
            report_df.groupby("ticker")
            .apply(
                lambda g: pd.Series(
                    {
                        "quantity": g["quantity"].sum(),
                        "buy_date": g["buy_date"].min(),
                        "sell_date": g["sell_date"].max(),
                        "total_cost_ars": g["total_cost_ars"].sum(),
                        "total_revenue_ars": g["total_revenue_ars"].sum(),
                        "total_cost_usd": g["total_cost_usd"].sum(),
                        "total_revenue_usd": g["total_revenue_usd"].sum(),
                        "real_return_ars_pct": weighted_avg(
                            g, "real_return_ars_pct", "total_cost_ars"
                        ),
                        "real_return_usd_pct": weighted_avg(
                            g, "real_return_usd_pct", "total_cost_usd"
                        ),
                    }
                )
            )
            .reset_index()
        )
        consolidated_df["nominal_return_ars_pct"] = (
            (consolidated_df["total_revenue_ars"] - consolidated_df["total_cost_ars"])
            / consolidated_df["total_cost_ars"]
        ) * 100
        consolidated_df["nominal_return_usd_pct"] = (
            (consolidated_df["total_revenue_usd"] - consolidated_df["total_cost_usd"])
            / consolidated_df["total_cost_usd"]
        ) * 100
        return consolidated_df
