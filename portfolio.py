import pandas as pd
import time
from services.market_data_service import MarketDataService


class Portfolio:
    positions_file = "data/open_positions.csv"
    trades_file = "data/closed_trades.csv"
    dolar_mep_file = "data/dolar_mep.csv"
    dolar_ccl_file = "data/dolar_ccl.csv"
    cpi_arg_file = "data/cpi_argentina.csv"
    cpi_usa_file = "data/cpi_usa.csv"

    def __init__(self):
        self.load_data()
        self.market_data_service = MarketDataService()

    def load_data(self):
        try:
            self.open_positions = pd.read_csv(
                self.positions_file, parse_dates=["purchase_date"]
            )
        except FileNotFoundError:
            self.open_positions = pd.DataFrame(
                columns=[
                    "purchase_id",
                    "purchase_date",
                    "ticker",
                    "quantity",
                    "total_cost_ars",
                    "total_cost_usd",
                    "asset_type",
                    "original_price",
                    "original_currency",
                ]
            )
        try:
            self.closed_trades = pd.read_csv(self.trades_file)
        except FileNotFoundError:
            self.closed_trades = pd.DataFrame(
                columns=[
                    "ticker",
                    "quantity",
                    "buy_date",
                    "sell_date",
                    "total_cost_ars",
                    "total_revenue_ars",
                    "total_cost_usd",
                    "total_revenue_usd",
                    "nominal_return_ars_pct",
                    "real_return_ars_pct",
                    "nominal_return_usd_pct",
                    "real_return_usd_pct",
                ]
            )
        # ... (resto de la carga de datos sin cambios)
        try:
            self.dolar_mep = pd.read_csv(self.dolar_mep_file, parse_dates=["date"])
            self.dolar_mep["date"] = pd.to_datetime(
                self.dolar_mep["date"], format="%Y-%m-%d"
            )
        except FileNotFoundError:
            self.dolar_mep = pd.DataFrame(columns=["date", "value"])
        try:
            self.dolar_ccl = pd.read_csv(self.dolar_ccl_file, parse_dates=["date"])
            self.dolar_ccl["date"] = pd.to_datetime(
                self.dolar_ccl["date"], format="%Y-%m-%d"
            )
        except FileNotFoundError:
            self.dolar_ccl = pd.DataFrame(columns=["date", "value"])
        try:
            self.cpi_arg = pd.read_csv(self.cpi_arg_file, parse_dates=["date"])
            self.cpi_arg["date"] = pd.to_datetime(
                self.cpi_arg["date"], format="%Y-%m-%d"
            )
        except FileNotFoundError:
            self.cpi_arg = pd.DataFrame(columns=["date", "value"])
        try:
            self.cpi_usa = pd.read_csv(self.cpi_usa_file, parse_dates=["date"])
            self.cpi_usa["date"] = pd.to_datetime(
                self.cpi_usa["date"], format="%Y-%m-%d"
            )
        except FileNotFoundError:
            self.cpi_usa = pd.DataFrame(columns=["date", "value"])

    def save_data(self):
        self.open_positions.to_csv(self.positions_file, index=False)
        self.closed_trades.to_csv(self.trades_file, index=False)

    def _get_closest_rate_from_df(self, date, rate_df):
        if rate_df.empty:
            return None
        rate_df.sort_values("date", inplace=True)
        date_df = pd.DataFrame({"date": [pd.to_datetime(date)]})
        merged = pd.merge_asof(date_df, rate_df, on="date", direction="nearest")
        return (
            merged["value"].iloc[0]
            if not merged.empty and not pd.isna(merged["value"].iloc[0])
            else None
        )

    def get_rate_for_asset(self, date, asset_type):
        asset_type = str(asset_type).upper()
        if asset_type in ["ACCION", "RF"]:
            return self._get_closest_rate_from_df(date, self.dolar_mep)
        elif asset_type == "CEDEAR":
            return self._get_closest_rate_from_df(date, self.dolar_ccl)
        else:
            print(
                f"Error: Invalid asset type '{asset_type}'. Cannot determine exchange rate."
            )
            return None

    def record_buy(
        self,
        date_str,
        ticker,
        quantity,
        currency,
        price,
        market_fees,
        taxes,
        asset_type,
    ):
        date = pd.to_datetime(date_str, format="%Y-%m-%d")
        rate = self.get_rate_for_asset(date, asset_type)

        if not rate and currency.upper() == "USD":
            print(
                f"Error: Exchange rate for {asset_type} not found for date {date}. Cannot record USD purchase."
            )
            return

        total_fees_ars = market_fees + taxes
        total_cost_ars, total_cost_usd = None, None

        if currency.upper() == "ARS":
            total_cost_ars = (quantity * price) + total_fees_ars
            if rate and rate > 0:
                total_cost_usd = total_cost_ars / rate
        elif currency.upper() == "USD":
            total_cost_usd = (quantity * price) + (total_fees_ars / rate)
            total_cost_ars = total_cost_usd * rate

        purchase_id = int(time.time() * 1000)
        new_lot = pd.DataFrame(
            {
                "purchase_id": [purchase_id],
                "purchase_date": [date],
                "ticker": [ticker],
                "quantity": [quantity],
                "total_cost_ars": [total_cost_ars],
                "total_cost_usd": [total_cost_usd],
                "asset_type": [asset_type],
                "original_price": [price],
                "original_currency": [currency.upper()],
            }
        )

        # Compatibilidad con CSVs viejos
        for col in new_lot.columns:
            if col not in self.open_positions.columns:
                self.open_positions[col] = None

        self.open_positions = pd.concat(
            [self.open_positions, new_lot], ignore_index=True
        )
        self.save_data()
        print(
            f"New lot of {quantity} {ticker} purchased on {date.strftime('%d-%m-%Y')} recorded successfully."
        )

    def record_sell(
        self,
        date_str,
        ticker,
        quantity_to_sell,
        currency,
        price,
        market_fees,
        taxes,
        asset_type,
    ):
        date = pd.to_datetime(date_str, format="%Y-%m-%d")
        ticker_lots = (
            self.open_positions[self.open_positions["ticker"] == ticker]
            .sort_values(by="purchase_date")
            .copy()
        )

        if ticker_lots.empty or ticker_lots["quantity"].sum() < quantity_to_sell:
            print(
                f"Error: Not enough shares to sell. You have {ticker_lots['quantity'].sum()}, trying to sell {quantity_to_sell}."
            )
            return

        rate = self.get_rate_for_asset(date, asset_type)
        if not rate:
            print(
                f"Error: Exchange rate for {asset_type} not found for date {date}. Cannot record sale."
            )
            return

        total_fees_ars = market_fees + taxes
        if currency.upper() == "ARS":
            total_revenue_ars = (quantity_to_sell * price) - total_fees_ars
            total_revenue_usd = total_revenue_ars / rate
        elif currency.upper() == "USD":
            total_revenue_usd = (quantity_to_sell * price) - (total_fees_ars / rate)
            total_revenue_ars = total_revenue_usd * rate
        else:
            total_revenue_ars, total_revenue_usd = 0, 0

        remaining_to_sell = quantity_to_sell
        for index, lot in ticker_lots.iterrows():
            if remaining_to_sell <= 0:
                break

            quantity_from_this_lot = min(lot["quantity"], remaining_to_sell)
            proportion_of_lot_sold = quantity_from_this_lot / lot["quantity"]

            # Calcular costo base proporcional a la venta
            cost_basis_ars = lot["total_cost_ars"] * proportion_of_lot_sold
            cost_basis_usd = (
                lot.get("total_cost_usd", 0) or 0
            ) * proportion_of_lot_sold

            proportion_of_total_sale = quantity_from_this_lot / quantity_to_sell
            revenue_for_sale_ars = total_revenue_ars * proportion_of_total_sale
            revenue_for_sale_usd = total_revenue_usd * proportion_of_total_sale

            lot_asset_type = lot.get("asset_type") or asset_type

            # Fallback para datos viejos que no tienen costo en USD
            if "total_cost_usd" not in lot or pd.isna(lot["total_cost_usd"]):
                buy_rate = self.get_rate_for_asset(lot["purchase_date"], lot_asset_type)
                cost_basis_usd = cost_basis_ars / buy_rate if buy_rate else 0

            self.log_closed_trade(
                ticker,
                quantity_from_this_lot,
                lot["purchase_date"],
                date,
                cost_basis_ars,
                cost_basis_usd,
                revenue_for_sale_ars,
                revenue_for_sale_usd,
                lot_asset_type,
            )

            if quantity_from_this_lot < lot["quantity"]:
                self.open_positions.loc[index, "quantity"] -= quantity_from_this_lot
                self.open_positions.loc[index, "total_cost_ars"] -= cost_basis_ars
                if "total_cost_usd" in self.open_positions.columns and pd.notna(
                    cost_basis_usd
                ):
                    self.open_positions.loc[index, "total_cost_usd"] -= cost_basis_usd
            else:
                self.open_positions.drop(index, inplace=True)

            remaining_to_sell -= quantity_from_this_lot

        self.save_data()
        print(
            f"Sale of {quantity_to_sell} {ticker} recorded successfully using FIFO logic."
        )

    def log_closed_trade(
        self,
        ticker,
        quantity,
        buy_date,
        sell_date,
        total_cost_ars,
        total_cost_usd,
        total_revenue_ars,
        total_revenue_usd,
        asset_type,
    ):
        nominal_return_ars_pct = (
            (total_revenue_ars / total_cost_ars - 1) if total_cost_ars > 0 else 0
        )
        nominal_return_usd_pct = (
            (total_revenue_usd / total_cost_usd - 1) if total_cost_usd > 0 else 0
        )

        inflation_arg = self.get_inflation(buy_date, sell_date, self.cpi_arg)
        real_return_ars_pct = (
            ((1 + nominal_return_ars_pct) / (1 + inflation_arg) - 1)
            if (1 + inflation_arg) != 0
            else nominal_return_ars_pct
        )

        inflation_usa = self.get_inflation(buy_date, sell_date, self.cpi_usa)
        real_return_usd_pct = (
            ((1 + nominal_return_usd_pct) / (1 + inflation_usa) - 1)
            if (1 + inflation_usa) != 0
            else nominal_return_usd_pct
        )

        trade_log = pd.DataFrame(
            {
                "ticker": [ticker],
                "quantity": [quantity],
                "buy_date": [buy_date.strftime("%d-%m-%Y")],
                "sell_date": [sell_date.strftime("%d-%m-%Y")],
                "total_cost_ars": [total_cost_ars],
                "total_revenue_ars": [total_revenue_ars],
                "total_cost_usd": [total_cost_usd],
                "total_revenue_usd": [total_revenue_usd],
                "nominal_return_ars_pct": [nominal_return_ars_pct * 100],
                "real_return_ars_pct": [real_return_ars_pct * 100],
                "nominal_return_usd_pct": [nominal_return_usd_pct * 100],
                "real_return_usd_pct": [real_return_usd_pct * 100],
            }
        )

        for col in trade_log.columns:
            if col not in self.closed_trades.columns:
                self.closed_trades[col] = None
        self.closed_trades = pd.concat(
            [self.closed_trades, trade_log], ignore_index=True
        )

    def show_open_positions(self):
        if self.open_positions.empty:
            print("No open positions.")
            return
        print("\n--- OPEN POSITIONS (BY PURCHASE LOT) ---")
        display_df = self.open_positions.copy()
        display_df["purchase_date"] = display_df["purchase_date"].dt.strftime(
            "%d-%m-%Y"
        )

        # Muestra los costos totales, reflejando el nuevo modelo de datos
        cols_to_display = [
            "purchase_date",
            "ticker",
            "quantity",
            "total_cost_ars",
            "total_cost_usd",
        ]

        # Compatibilidad con archivos viejos
        for col in cols_to_display:
            if col not in display_df.columns:
                display_df[col] = None  # o pd.NA

        print(display_df[cols_to_display].round(2).to_string(index=False))

    def show_closed_trades(self):
        if self.closed_trades.empty:
            print("No closed trades recorded.")
        else:
            print("\n--- CLOSED TRADES HISTORY ---")
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
            # Filtrar para mostrar solo las columnas deseadas
            display_df = self.closed_trades[
                [col for col in display_cols if col in self.closed_trades.columns]
            ].rename(columns=display_cols)
            # Formato de fecha consistente
            display_df["Buy Date"] = pd.to_datetime(
                display_df["Buy Date"], format="%d-%m-%Y"
            ).dt.strftime("%d-%m-%Y")
            display_df["Sell Date"] = pd.to_datetime(
                display_df["Sell Date"], format="%d-%m-%Y"
            ).dt.strftime("%d-%m-%Y")
            print(display_df.round(2).to_string(index=False))

    def get_inflation(self, start_date, end_date, cpi_df):
        if cpi_df.empty:
            return 0.0
        cpi_df.sort_values("date", inplace=True)
        start_date_df = pd.DataFrame({"date": [pd.to_datetime(start_date)]})
        end_date_df = pd.DataFrame({"date": [pd.to_datetime(end_date)]})
        start_cpi_row = pd.merge_asof(
            start_date_df, cpi_df, on="date", direction="nearest"
        )
        end_cpi_row = pd.merge_asof(end_date_df, cpi_df, on="date", direction="nearest")
        if (
            start_cpi_row.empty
            or end_cpi_row.empty
            or pd.isna(start_cpi_row["value"].iloc[0])
            or pd.isna(end_cpi_row["value"].iloc[0])
        ):
            return 0.0
        start_val, end_val = (
            start_cpi_row["value"].iloc[0],
            end_cpi_row["value"].iloc[0],
        )
        return end_val / start_val if start_val != 0 else 0.0
