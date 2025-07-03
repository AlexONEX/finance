import pandas as pd
from src.domain.portfolio import Portfolio
from src.infrastructure.persistence.portfolio_repository import PortfolioRepository


class TransactionService:
    def __init__(self, portfolio: Portfolio, repository: PortfolioRepository):
        self.portfolio = portfolio
        self.repository = repository

    def _get_exchange_rate(self, date: pd.Timestamp, asset_type: str) -> float | None:
        """Gets the appropriate exchange rate for a given date."""
        rate_df = (
            self.portfolio.dolar_ccl
            if asset_type == "CEDEAR"
            else self.portfolio.dolar_mep
        )
        if rate_df.empty:
            return None

        merged = pd.merge_asof(
            pd.DataFrame({"date": [date]}),
            rate_df.sort_values("date"),
            on="date",
            direction="nearest",
        )
        return merged["value"].iloc[0] if not merged.empty else None

    def record_buy(self, details: dict):
        """Records a new buy transaction, adds it to open positions, and saves."""
        rate = self._get_exchange_rate(details["date"], details["asset_type"])
        if not rate:
            raise ValueError(f"Could not find exchange rate for date {details['date']}")

        cost = (
            (details["quantity"] * details["price"])
            + details["market_fees"]
            + details["taxes"]
        )

        cost_ars, cost_usd = (
            (cost, cost / rate) if details["currency"] == "ARS" else (cost * rate, cost)
        )

        new_position = {
            "purchase_id": pd.Timestamp.now().strftime("%Y%m%d%H%M%S%f"),
            "purchase_date": details["date"],
            "ticker": details["ticker"],
            "quantity": details["quantity"],
            "total_cost_ars": cost_ars,
            "total_cost_usd": cost_usd,
            "asset_type": details["asset_type"],
            "original_price": details["price"],
            "original_currency": details["currency"],
        }

        new_df = pd.DataFrame([new_position])
        self.portfolio.open_positions = pd.concat(
            [self.portfolio.open_positions, new_df], ignore_index=True
        )
        self.repository.save_open_positions(self.portfolio.open_positions)

    def record_sell(self, details: dict):
        """Records a sell, matches it against open lots (FIFO), and updates state."""
        open_lots = self.portfolio.open_positions
        matching_lots = (
            open_lots[open_lots["ticker"] == details["ticker"]]
            .sort_values(by="purchase_date")
            .copy()
        )

        if matching_lots["quantity"].sum() < details["quantity"]:
            raise ValueError(f"Not enough quantity of {details['ticker']} to sell.")

        rate = self._get_exchange_rate(details["date"], details["asset_type"])
        revenue = (
            (details["quantity"] * details["price"])
            - details["market_fees"]
            - details["taxes"]
        )
        revenue_ars, revenue_usd = (
            (revenue, revenue / rate)
            if details["currency"] == "ARS"
            else (revenue * rate, revenue)
        )

        quantity_to_sell = details["quantity"]
        newly_closed_trades = []

        for index, lot in matching_lots.iterrows():
            if quantity_to_sell <= 0:
                break

            qty_from_lot = min(lot["quantity"], quantity_to_sell)
            proportion = qty_from_lot / lot["quantity"]

            closed_trade = {
                "ticker": lot["ticker"],
                "quantity": qty_from_lot,
                "buy_date": lot["purchase_date"],
                "sell_date": details["date"],
                "total_cost_ars": lot["total_cost_ars"] * proportion,
                "total_cost_usd": lot["total_cost_usd"] * proportion,
                "total_revenue_ars": revenue_ars * (qty_from_lot / details["quantity"]),
                "total_revenue_usd": revenue_usd * (qty_from_lot / details["quantity"]),
            }
            newly_closed_trades.append(closed_trade)

            # Update open positions dataframe
            open_lots.loc[index, "quantity"] -= qty_from_lot
            open_lots.loc[index, "total_cost_ars"] -= closed_trade["total_cost_ars"]
            open_lots.loc[index, "total_cost_usd"] -= closed_trade["total_cost_usd"]
            quantity_to_sell -= qty_from_lot

        # Remove fully sold lots and save
        self.portfolio.open_positions = open_lots[open_lots["quantity"] > 0.001]
        self.repository.save_open_positions(self.portfolio.open_positions)

        # Append new closed trades and save
        new_closed_df = pd.DataFrame(newly_closed_trades)
        self.portfolio.closed_trades = pd.concat(
            [self.portfolio.closed_trades, new_closed_df], ignore_index=True
        )
        self.repository.save_closed_trades(self.portfolio.closed_trades)
