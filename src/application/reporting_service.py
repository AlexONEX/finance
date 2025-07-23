import pandas as pd
import logging
import config
import re
from src.domain.portfolio import Portfolio
from src.infrastructure.gateways.instances import data912_connector
from src.shared.financial_utils import calculate_inflation_period


class ReportingService:
    def __init__(self, portfolio: Portfolio):
        self.portfolio = portfolio
        self.api_connector = data912_connector
        self.price_cache = {}

    def _get_live_prices_by_type(self, asset_type: str):
        """
        Fetches live prices from the API based on a unified mapping of asset types.
        Caches results to avoid redundant calls.
        """
        asset_type = asset_type.upper()

        # Mapping from our specific asset types to API endpoint functions
        FETCHER_MAP = {
            "CEDEAR": self.api_connector.get_arg_cedears,
            "OPTION": self.api_connector.get_arg_options,
            "ACCION": self.api_connector.get_arg_stocks,
            "GENERAL": self.api_connector.get_arg_stocks,
            "MERVAL": self.api_connector.get_arg_stocks,
            "LIDER": self.api_connector.get_arg_stocks,
            "PRIVATE_TITLE": self.api_connector.get_arg_stocks,
        }

        FIXED_INCOME_TYPES = ["BOND", "LETTER", "PUBLIC_TITLE", "RF", "ON"]
        cache_key = "fixed_income" if asset_type in FIXED_INCOME_TYPES else asset_type

        if cache_key in self.price_cache:
            return self.price_cache[cache_key]

        logging.info(f"Buscando precios en vivo para el grupo: {cache_key}...")

        all_prices = {}
        if cache_key == "fixed_income":
            fetch_functions = [
                self.api_connector.get_arg_bonds,
                self.api_connector.get_arg_notes,
                self.api_connector.get_arg_corporate_debt,
            ]
            for fetch in fetch_functions:
                live_data = fetch()
                if isinstance(live_data, list):
                    prices = {
                        item["symbol"].upper(): item.get("c", 0)
                        for item in live_data
                        if "symbol" in item
                    }
                    all_prices.update(prices)
        else:
            fetch_function = FETCHER_MAP.get(asset_type)
            if fetch_function:
                live_data = fetch_function()
                if isinstance(live_data, list):
                    all_prices = {
                        re.sub(r"[\s.,()]", "", item["symbol"]).upper(): item.get(
                            "c", 0
                        )
                        for item in live_data
                        if "symbol" in item
                    }

        self.price_cache[cache_key] = all_prices
        return all_prices

    def _get_current_price(self, asset_type: str, ticker: str) -> float | None:
        if pd.isna(asset_type) or pd.isna(ticker):
            return None

        price_dict = self._get_live_prices_by_type(asset_type)
        sanitized_ticker = re.sub(r"[\s.,()]", "", ticker).upper()
        price = price_dict.get(sanitized_ticker)

        if price is None:
            logging.warning(
                f"No se encontró precio en vivo para el ticker: {ticker} (Buscado como: {sanitized_ticker})"
            )
            return None

        # For fixed income, the price from the API is per 100 V/N
        if asset_type.upper() in ["BOND", "LETTER", "PUBLIC_TITLE", "RF", "ON"]:
            return float(price) / config.BOND_PRICE_DIVISOR

        return float(price)

    def generate_open_positions_report(self) -> dict:
        if self.portfolio.open_positions.empty:
            return {"consolidated": pd.DataFrame(), "options": pd.DataFrame()}

        self.price_cache = {}  # Reset cache for each report run
        positions = self.portfolio.open_positions.copy()

        # Separate options first, as they are not consolidated
        options_positions = positions[positions["asset_type"] == "OPTION"]
        positions = positions[positions["asset_type"] != "OPTION"].copy()

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
                        "asset_type": x["asset_type"].iloc[0],
                        "first_purchase_date": x["purchase_date"].min(),
                    }
                ),
                include_groups=False,  # Silences the FutureWarning
            )
            .reset_index()
        )

        consolidated["buy_price_ars"] = (
            consolidated["total_cost_ars"] / consolidated["quantity"]
        )
        consolidated["current_price"] = consolidated.apply(
            lambda row: self._get_current_price(row["asset_type"], row["ticker"]),
            axis=1,
        )

        # Drop rows where a price could not be found to avoid errors in calculation
        consolidated.dropna(subset=["current_price"], inplace=True)

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
        return {"consolidated": consolidated, "options": options_positions}

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
                ),
                include_groups=False,
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
