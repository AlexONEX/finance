import pandas as pd
import config
from functools import lru_cache
from src.domain.portfolio import Portfolio
from src.infrastructure.persistence.portfolio_repository import PortfolioRepository
from src.shared.types import TransactionData


class TransactionService:
    def __init__(self, portfolio: Portfolio, repository: PortfolioRepository):
        self.portfolio = portfolio
        self.repository = repository

    @lru_cache(maxsize=None)
    def _get_exchange_rate(self, date: pd.Timestamp, asset_type: str) -> float | None:
        rate_df = (
            self.portfolio.dolar_ccl
            if asset_type == "CEDEAR"
            else self.portfolio.dolar_mep
        )
        if rate_df.empty:
            return None
        merged = pd.merge_asof(
            pd.DataFrame({"date": [date]}),
            rate_df.sort_values(by="date"),
            on="date",
            direction="nearest",
        )
        return merged["value"].iloc[0] if not merged.empty else None

    def record_buy(self, details: TransactionData):
        asset_type = details["asset_type"].upper()
        original_price = details["price"]
        quantity = details["quantity"]
        instrument_category = details.get("instrument_type", asset_type).upper()

        base_cost = 0
        if instrument_category in [
            "ACCION",
            "CEDEAR",
            "MERVAL",
            "GENERAL",
            "LIDER",
            "PRIVATE_TITLE",
        ]:
            base_cost = quantity * original_price
        elif instrument_category in ["RF", "BOND", "LETTER", "PUBLIC_TITLE"]:
            base_cost = quantity * original_price
        elif instrument_category == "OPTION":
            base_cost = quantity * original_price * config.OPTION_LOT_SIZE
        else:
            raise ValueError(
                f"Asset type '{instrument_category}' not recognized for buy logic."
            )

        total_fees = (
            details["market_fees"] + details.get("broker_fees", 0) + details["taxes"]
        )
        cost = base_cost + total_fees
        rate = self._get_exchange_rate(details["date"], asset_type)
        if not rate:
            raise ValueError(f"Could not find exchange rate for date {details['date']}")
        cost_ars, cost_usd = (
            (cost, cost / rate) if details["currency"] == "ARS" else (cost * rate, cost)
        )

        new_position = {
            "purchase_date": details["date"],
            "ticker": details["ticker"],
            "quantity": details["quantity"],
            "total_cost_ars": cost_ars,
            "total_cost_usd": cost_usd,
            "asset_type": details.get("instrument_type", asset_type),
            "original_currency": details["currency"],
            "lotes": details["quantity"] if instrument_category == "OPTION" else None,
            "expiration_date": details.get("expiration_date"),
            "market_fees": details["market_fees"],
            "broker_fees": details.get("broker_fees", 0),
            "taxes": details["taxes"],
            "broker_transaction_id": details.get("broker_transaction_id"),
        }
        new_df = pd.DataFrame([new_position])
        if self.portfolio.open_positions.empty:
            self.portfolio.open_positions = new_df
        else:
            self.portfolio.open_positions = pd.concat(
                [self.portfolio.open_positions, new_df], ignore_index=True
            )
        self.repository.save_open_positions(self.portfolio.open_positions)

    def record_sell(self, details: dict):
        open_lots = self.portfolio.open_positions
        filtered_lots: pd.DataFrame = open_lots[
            open_lots["ticker"] == details["ticker"]
        ]
        matching_lots: pd.DataFrame = filtered_lots.sort_values(
            by="purchase_date"
        ).copy()

        if matching_lots["quantity"].sum() < details["quantity"]:
            raise ValueError(f"Not enough quantity of {details['ticker']} to sell.")

        rate = self._get_exchange_rate(details["date"], details["asset_type"])
        if not rate:
            raise ValueError(f"Could not find exchange rate for date {details['date']}")

        original_price = details["price"]
        quantity = details["quantity"]
        asset_type = details["asset_type"].upper()
        instrument_category = details.get("instrument_type", asset_type).upper()

        gross_revenue = 0
        if instrument_category in [
            "ACCION",
            "CEDEAR",
            "MERVAL",
            "GENERAL",
            "LIDER",
            "PRIVATE_TITLE",
        ]:
            gross_revenue = quantity * original_price
        elif instrument_category in ["RF", "BOND", "LETTER", "PUBLIC_TITLE"]:
            gross_revenue = quantity * original_price
        elif instrument_category == "OPTION":
            gross_revenue = quantity * original_price * config.OPTION_LOT_SIZE
        else:
            raise ValueError(
                f"Asset type '{instrument_category}' not recognized for sell logic."
            )

        total_fees = (
            details["market_fees"] + details.get("broker_fees", 0) + details["taxes"]
        )
        revenue = gross_revenue - total_fees
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
            qty_from_lot = min(float(lot["quantity"]), quantity_to_sell)
            proportion = qty_from_lot / float(lot["quantity"])
            closed_trade = {
                "ticker": lot["ticker"],
                "quantity": qty_from_lot,
                "buy_date": lot["purchase_date"],
                "sell_date": details["date"],
                "asset_type": lot.get("asset_type", "UNKNOWN"),
                "total_cost_ars": lot["total_cost_ars"] * proportion,
                "total_cost_usd": lot["total_cost_usd"] * proportion,
                "total_revenue_ars": revenue_ars * (qty_from_lot / details["quantity"]),
                "total_revenue_usd": revenue_usd * (qty_from_lot / details["quantity"]),
                "buy_broker_transaction_id": lot.get("broker_transaction_id"),
                "sell_broker_transaction_id": details.get("broker_transaction_id"),
            }
            newly_closed_trades.append(closed_trade)
            open_lots.loc[index, "quantity"] -= qty_from_lot
            open_lots.loc[index, "total_cost_ars"] -= closed_trade["total_cost_ars"]
            open_lots.loc[index, "total_cost_usd"] -= closed_trade["total_cost_usd"]
            quantity_to_sell -= qty_from_lot

        self.portfolio.open_positions = open_lots.loc[
            open_lots["quantity"] > 0.001
        ].copy()
        self.repository.save_open_positions(self.portfolio.open_positions)
        new_closed_df = pd.DataFrame(newly_closed_trades)
        if self.portfolio.closed_trades.empty:
            self.portfolio.closed_trades = new_closed_df
        else:
            self.portfolio.closed_trades = pd.concat(
                [self.portfolio.closed_trades, new_closed_df], ignore_index=True
            )
        self.repository.save_closed_trades(self.portfolio.closed_trades)

    def expire_options(self):
        today = pd.Timestamp.now().normalize()
        if (
            self.portfolio.open_positions.empty
            or "asset_type" not in self.portfolio.open_positions.columns
        ):
            return

        option_mask = self.portfolio.open_positions["asset_type"] == "OPTION"
        if not option_mask.any():
            return

        open_options = self.portfolio.open_positions[option_mask].copy()
        if open_options.empty:
            return

        open_options["expiration_date"] = pd.to_datetime(
            open_options["expiration_date"], errors="coerce"
        )
        expired: pd.DataFrame = open_options[
            open_options["expiration_date"] < today
        ].copy()
        if expired.empty:
            return

        newly_closed_trades = []
        for _, lot in expired.iterrows():
            closed_trade = {
                "ticker": lot["ticker"],
                "quantity": lot["quantity"],
                "buy_date": lot["purchase_date"],
                "sell_date": lot["expiration_date"],
                "asset_type": "OPTION",
                "total_cost_ars": lot["total_cost_ars"],
                "total_cost_usd": lot["total_cost_usd"],
                "total_revenue_ars": 0,
                "total_revenue_usd": 0,
                "buy_broker_transaction_id": lot.get("broker_transaction_id"),
                "sell_broker_transaction_id": "EXPIRED",
            }
            newly_closed_trades.append(closed_trade)

        expired_indices = expired.index
        self.portfolio.open_positions = self.portfolio.open_positions.drop(
            expired_indices
        )

        if newly_closed_trades:
            new_closed_df = pd.DataFrame(newly_closed_trades)
            if self.portfolio.closed_trades.empty:
                self.portfolio.closed_trades = new_closed_df
            else:
                self.portfolio.closed_trades = pd.concat(
                    [self.portfolio.closed_trades, new_closed_df], ignore_index=True
                )
            self.repository.save_open_positions(self.portfolio.open_positions)
            self.repository.save_closed_trades(self.portfolio.closed_trades)
            print(f"INFO: Se procesaron {len(newly_closed_trades)} opciones expiradas.")
