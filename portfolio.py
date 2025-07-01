import pandas as pd
from datetime import datetime


class Portfolio:
    def __init__(self):
        self.positions_file = "open_positions.csv"
        self.trades_file = "closed_trades.csv"
        self.rates_file = "exchange_rates.csv"
        self.cpi_arg_file = "cpi_argentina.csv"
        self.cpi_usa_file = "cpi_usa.csv"
        self.load_data()

    def load_data(self):
        try:
            self.open_positions = pd.read_csv(self.positions_file)
        except FileNotFoundError:
            self.open_positions = pd.DataFrame(
                columns=["ticker", "quantity", "avg_price_ars"]
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
                    "nominal_return_ars_pct",
                    "real_return_ars_pct",
                    "nominal_return_usd_pct",
                    "real_return_usd_pct",
                ]
            )
        try:
            self.exchange_rates = pd.read_csv(self.rates_file, parse_dates=["date"])
        except FileNotFoundError:
            self.exchange_rates = pd.DataFrame(columns=["date", "rate"])
        try:
            self.cpi_arg = pd.read_csv(self.cpi_arg_file, parse_dates=["date"])
        except FileNotFoundError:
            self.cpi_arg = pd.DataFrame(columns=["date", "value"])
        try:
            self.cpi_usa = pd.read_csv(self.cpi_usa_file, parse_dates=["date"])
        except FileNotFoundError:
            self.cpi_usa = pd.DataFrame(columns=["date", "value"])

    def save_data(self):
        self.open_positions.to_csv(self.positions_file, index=False)
        self.closed_trades.to_csv(self.trades_file, index=False)

    def get_closest_rate(self, date):
        if self.exchange_rates.empty:
            return None
        self.exchange_rates.sort_values("date", inplace=True)
        date_df = pd.DataFrame({"date": [pd.to_datetime(date)]})
        merged = pd.merge_asof(
            date_df, self.exchange_rates, on="date", direction="nearest"
        )
        return (
            merged["rate"].iloc[0]
            if not merged.empty and not pd.isna(merged["rate"].iloc[0])
            else None
        )

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
        start_val = start_cpi_row["value"].iloc[0]
        end_val = end_cpi_row["value"].iloc[0]
        if start_val == 0:
            return 0.0
        return (end_val / start_val) - 1

    def record_buy(self, date, ticker, quantity, currency, price):
        price_ars = price
        if currency.upper() == "USD":
            rate = self.get_closest_rate(date)
            if not rate:
                print(
                    f"Error: Exchange rate not found for date {date}. Cannot record purchase."
                )
                return
            price_ars = price * rate
        total_cost = quantity * price_ars
        if ticker in self.open_positions["ticker"].values:
            idx = self.open_positions.index[self.open_positions["ticker"] == ticker][0]
            old_quantity = self.open_positions.at[idx, "quantity"]
            old_avg_price = self.open_positions.at[idx, "avg_price_ars"]
            old_total_cost = old_quantity * old_avg_price
            new_quantity = old_quantity + quantity
            new_total_cost = old_total_cost + total_cost
            new_avg_price = new_total_cost / new_quantity
            self.open_positions.at[idx, "quantity"] = new_quantity
            self.open_positions.at[idx, "avg_price_ars"] = new_avg_price
        else:
            new_position = pd.DataFrame(
                {
                    "ticker": [ticker],
                    "quantity": [quantity],
                    "avg_price_ars": [price_ars],
                }
            )
            self.open_positions = pd.concat(
                [self.open_positions, new_position], ignore_index=True
            )
        self.save_data()
        print(f"Purchase of {quantity} {ticker} recorded successfully.")

    def record_sell(self, date, ticker, quantity, currency, price):
        if ticker not in self.open_positions["ticker"].values:
            print(f"Error: No open position for {ticker}.")
            return
        idx = self.open_positions.index[self.open_positions["ticker"] == ticker][0]
        position_quantity = self.open_positions.at[idx, "quantity"]
        if quantity > position_quantity:
            print(
                f"Error: Attempting to sell {quantity} of {ticker}, but you only own {position_quantity}."
            )
            return

        avg_cost_price_ars = self.open_positions.at[idx, "avg_price_ars"]
        total_cost_ars = quantity * avg_cost_price_ars
        sell_price_ars = price
        if currency.upper() == "USD":
            rate = self.get_closest_rate(date)
            if not rate:
                print(
                    f"Error: Exchange rate not found for date {date}. Cannot record sale."
                )
                return
            sell_price_ars = price * rate
        total_revenue_ars = quantity * sell_price_ars

        # A simplification: using today's date as the effective 'buy date' for inflation calculation.
        buy_date = datetime.now().strftime("%Y-%m-%d")
        nominal_return_ars_pct = (
            (total_revenue_ars / total_cost_ars - 1) if total_cost_ars > 0 else 0
        )

        buy_rate = self.get_closest_rate(buy_date)
        sell_rate = self.get_closest_rate(date)
        if not buy_rate or not sell_rate:
            print(
                "Warning: Could not calculate USD return due to missing exchange rate data."
            )
            nominal_return_usd_pct = 0.0
        else:
            total_cost_usd = total_cost_ars / buy_rate
            total_revenue_usd = total_revenue_ars / sell_rate
            nominal_return_usd_pct = (
                (total_revenue_usd / total_cost_usd - 1) if total_cost_usd > 0 else 0
            )

        inflation_arg = self.get_inflation(buy_date, date, self.cpi_arg)
        real_return_ars_pct = (
            ((1 + nominal_return_ars_pct) / (1 + inflation_arg) - 1)
            if (1 + inflation_arg) != 0
            else nominal_return_ars_pct
        )

        inflation_usa = self.get_inflation(buy_date, date, self.cpi_usa)
        real_return_usd_pct = (
            ((1 + nominal_return_usd_pct) / (1 + inflation_usa) - 1)
            if (1 + inflation_usa) != 0
            else nominal_return_usd_pct
        )

        trade_log = pd.DataFrame(
            {
                "ticker": [ticker],
                "quantity": [quantity],
                "buy_date": [buy_date],
                "sell_date": [date],
                "total_cost_ars": [total_cost_ars],
                "total_revenue_ars": [total_revenue_ars],
                "nominal_return_ars_pct": [nominal_return_ars_pct * 100],
                "real_return_ars_pct": [real_return_ars_pct * 100],
                "nominal_return_usd_pct": [nominal_return_usd_pct * 100],
                "real_return_usd_pct": [real_return_usd_pct * 100],
            }
        )
        self.closed_trades = pd.concat(
            [self.closed_trades, trade_log], ignore_index=True
        )

        remaining_quantity = position_quantity - quantity
        if remaining_quantity > 0:
            self.open_positions.at[idx, "quantity"] = remaining_quantity
        else:
            self.open_positions.drop(index=idx, inplace=True)
        self.save_data()
        print(f"Sale of {quantity} {ticker} recorded. Performance calculated.")

    def show_open_positions(self):
        if self.open_positions.empty:
            print("No open positions.")
        else:
            print("\n--- OPEN POSITIONS ---")
            print(self.open_positions.round(2).to_string(index=False))

    def show_closed_trades(self):
        if self.closed_trades.empty:
            print("No closed trades recorded.")
        else:
            print("\n--- CLOSED TRADES HISTORY ---")
            display_cols = {
                "ticker": "Ticker",
                "quantity": "Quantity",
                "sell_date": "Sell Date",
                "nominal_return_ars_pct": "Nom. Ret. ARS (%)",
                "real_return_ars_pct": "Real Ret. ARS (%)",
                "nominal_return_usd_pct": "Nom. Ret. USD (%)",
                "real_return_usd_pct": "Real Ret. USD (%)",
            }
            display_df = self.closed_trades[list(display_cols.keys())].rename(
                columns=display_cols
            )
            print(display_df.round(2).to_string(index=False))
