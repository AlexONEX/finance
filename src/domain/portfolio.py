import pandas as pd
import os
import config


class Portfolio:
    def __init__(self):
        """Initializes the Portfolio by loading all necessary data."""
        self.load_all_data()

    def _load_csv(
        self, file_path: str, columns: list, parse_dates: list = None
    ) -> pd.DataFrame:
        """Loads a single CSV file safely, returning an empty DataFrame on failure."""
        try:
            if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
                return pd.read_csv(file_path, parse_dates=parse_dates)
        except (FileNotFoundError, pd.errors.EmptyDataError):
            return pd.DataFrame(columns=columns)
        return pd.DataFrame(columns=columns)

    def load_all_data(self):
        """Loads all CSV files required for portfolio analysis."""
        open_positions_cols = [
            "purchase_id",
            "purchase_date",
            "ticker",
            "quantity",
            "total_cost_ars",
            "total_cost_usd",
            "asset_type",
            "original_price",
            "original_currency",
            "underlying_asset",
            "option_type",
            "strike_price",
            "expiration_date",
            "broker_transaction_id",
        ]
        closed_trades_cols = [
            "ticker",
            "quantity",
            "buy_date",
            "sell_date",
            "total_cost_ars",
            "total_revenue_ars",
            "total_cost_usd",
            "total_revenue_usd",
            "buy_broker_transaction_id",
            "sell_broker_transaction_id",
        ]

        self.open_positions = self._load_csv(
            config.OPEN_POSITIONS_FILE,
            open_positions_cols,
            ["purchase_date", "expiration_date"],
        )
        self.closed_trades = self._load_csv(
            config.CLOSED_TRADES_FILE, closed_trades_cols, ["buy_date", "sell_date"]
        )
        self.dolar_mep = self._load_csv(
            config.DOLAR_MEP_FILE, ["date", "value"], ["date"]
        )
        self.dolar_ccl = self._load_csv(
            config.DOLAR_CCL_FILE, ["date", "value"], ["date"]
        )
        self.cpi_arg = self._load_csv(config.CPI_ARG_FILE, ["date", "value"], ["date"])
        self.cpi_usa = self._load_csv(config.CPI_USA_FILE, ["date", "value"], ["date"])

    def get_inflation(self, start_date, end_date, cpi_df: pd.DataFrame) -> float:
        """Calculates cumulative inflation between two dates using a CPI DataFrame."""
        if cpi_df.empty or pd.isna(start_date) or pd.isna(end_date):
            return 0.0

        cpi_df = cpi_df.sort_values("date")
        start_cpi = pd.merge_asof(
            pd.DataFrame({"date": [pd.to_datetime(start_date)]}),
            cpi_df,
            on="date",
            direction="nearest",
        )
        end_cpi = pd.merge_asof(
            pd.DataFrame({"date": [pd.to_datetime(end_date)]}),
            cpi_df,
            on="date",
            direction="nearest",
        )

        if (
            start_cpi.empty
            or end_cpi.empty
            or pd.isna(start_cpi["value"].iloc[0])
            or pd.isna(end_cpi["value"].iloc[0])
        ):
            return 0.0

        start_val, end_val = start_cpi["value"].iloc[0], end_cpi["value"].iloc[0]
        return (end_val / start_val) - 1.0 if start_val > 0 else 0.0

    def show_open_positions(self):
        """Displays formatted open positions grouped by asset type."""
        if self.open_positions.empty:
            print("No open positions.")
            return

        display_df = self.open_positions.copy()
        base_cols = [
            "purchase_date",
            "ticker",
            "quantity",
            "total_cost_ars",
            "total_cost_usd",
        ]
        option_cols = [
            "underlying_asset",
            "option_type",
            "strike_price",
            "expiration_date",
        ]

        for col in base_cols + option_cols:
            if col not in display_df.columns:
                display_df[col] = pd.NA

        display_df["purchase_date"] = pd.to_datetime(
            display_df["purchase_date"]
        ).dt.strftime("%d-%m-%Y")
        if "expiration_date" in display_df.columns:
            display_df["expiration_date"] = pd.to_datetime(
                display_df["expiration_date"]
            ).dt.strftime("%d-%m-%Y")

        options_df = display_df[display_df["asset_type"] == "OPCION"]
        other_df = display_df[~display_df["asset_type"].isin(["OPCION"])]

        print("\n--- Stocks, CEDEARs, Bonds ---")
        print(
            other_df[base_cols].round(2).to_string(index=False)
            if not other_df.empty
            else "No positions."
        )
        print("\n--- Options ---")
        print(
            options_df[base_cols + option_cols]
            .fillna("")
            .round(2)
            .to_string(index=False)
            if not options_df.empty
            else "No options positions."
        )

    def show_closed_trades(self):
        """Calculates and displays the history of closed trades with performance metrics."""
        if self.closed_trades.empty:
            print("No closed trades recorded.")
            return

        df = self.closed_trades.copy()
        df["nominal_return_ars_pct"] = (
            df["total_revenue_ars"] / df["total_cost_ars"] - 1
        ) * 100
        df["nominal_return_usd_pct"] = (
            df["total_revenue_usd"] / df["total_cost_usd"] - 1
        ) * 100
        df["inflation_arg"] = df.apply(
            lambda row: self.get_inflation(
                row["buy_date"], row["sell_date"], self.cpi_arg
            ),
            axis=1,
        )
        df["inflation_usa"] = df.apply(
            lambda row: self.get_inflation(
                row["buy_date"], row["sell_date"], self.cpi_usa
            ),
            axis=1,
        )
        df["real_return_ars_pct"] = (
            ((1 + df["nominal_return_ars_pct"] / 100) / (1 + df["inflation_arg"])) - 1
        ) * 100
        df["real_return_usd_pct"] = (
            ((1 + df["nominal_return_usd_pct"] / 100) / (1 + df["inflation_usa"])) - 1
        ) * 100

        display_cols = {
            "ticker": "Ticker",
            "quantity": "Quantity",
            "buy_date": "Buy Date",
            "sell_date": "Sell Date",
            "nominal_return_ars_pct": "Nom. Ret. ARS (%)",
            "real_return_ars_pct": "Real Ret. ARS (%)",
            "nominal_return_usd_pct": "Nom. Ret. USD (%)",
            "real_return_usd_pct": "Real Ret. USD (%)",
        }
        df["buy_date"] = pd.to_datetime(df["buy_date"]).dt.strftime("%d-%m-%Y")
        df["sell_date"] = pd.to_datetime(df["sell_date"]).dt.strftime("%d-%m-%Y")

        display_df = df[list(display_cols.keys())].rename(columns=display_cols)
        print("\n--- CLOSED TRADES HISTORY ---")
        print(display_df.round(2).to_string(index=False))
