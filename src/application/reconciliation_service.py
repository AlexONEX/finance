import json
import pandas as pd
import logging
import os
import config
import numpy as np
from src.shared.financial_utils import (
    map_instrument_to_asset_type,
    parse_option_details,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class ExchangeRateLoader:
    """Loads and provides exchange rates from CSV files."""

    def __init__(self):
        self.dolar_mep = self._load_rate_file(config.DOLAR_MEP_FILE)
        self.dolar_ccl = self._load_rate_file(config.DOLAR_CCL_FILE)

    def _load_rate_file(self, file_path: str):
        try:
            return pd.read_csv(file_path, parse_dates=["date"])
        except FileNotFoundError:
            return pd.DataFrame(columns=["date", "value"])

    def get_rate(self, date, asset_type: str):
        """Gets the appropriate exchange rate for a given date and asset type."""
        rate_df = self.dolar_ccl if asset_type == "CEDEAR" else self.dolar_mep
        if rate_df.empty:
            return None
        merged = pd.merge_asof(
            pd.DataFrame({"date": [pd.to_datetime(date)]}),
            rate_df.sort_values("date"),
            on="date",
            direction="nearest",
        )
        return (
            merged["value"].iloc[0]
            if not merged.empty and pd.notna(merged["value"].iloc[0])
            else None
        )


def _load_processed_ids() -> set:
    """Loads all previously processed transaction IDs from portfolio CSVs."""
    processed_ids = set()
    files_to_check = {
        config.OPEN_POSITIONS_FILE: ["broker_transaction_id"],
        config.CLOSED_TRADES_FILE: [
            "buy_broker_transaction_id",
            "sell_broker_transaction_id",
        ],
    }
    for file_path, id_cols in files_to_check.items():
        try:
            if os.path.exists(file_path):
                df = pd.read_csv(file_path)
                for col in id_cols:
                    if col in df.columns:
                        processed_ids.update(df[col].dropna().astype(str))
        except (FileNotFoundError, pd.errors.EmptyDataError):
            logging.info(f"{file_path} not found or is empty.")
        except Exception as e:
            logging.error(f"Error reading {file_path} for IDs: {e}")
    return processed_ids


def _load_and_filter_new_transactions(processed_ids: set) -> list:
    """Loads transactions from JSON, filtering for new and valid entries."""
    try:
        with open(config.TRANSACTIONS_FILE, "r", encoding="utf-8") as f:
            all_transactions = json.load(f)
    except Exception as e:
        logging.error(f"Could not read or parse {config.TRANSACTIONS_FILE}: {e}")
        return []

    new_transactions = []
    for tx in all_transactions:
        tx_id = str(tx.get("id"))
        if (
            tx_id in processed_ids
            or tx.get("state") != "FULFILLED"
            or tx.get("orderOperation") not in ["BUY", "SELL"]
        ):
            continue
        try:
            instrument = tx.get("instrument", {})
            asset_type = map_instrument_to_asset_type(instrument)
            if asset_type == "UNKNOWN":
                continue

            # **NUEVA LÓGICA PARA COMISIONES E IMPUESTOS**
            total_gross = float(tx.get("totalGross", 0))
            total_net = float(tx.get("total", 0))
            commissions_data = tx.get("commissions", {})

            market_tariff_pct = (
                commissions_data.get("marketTariffPercentage", 0.0) / 100.0
            )
            broker_tariff_pct = commissions_data.get("tariffPercentage", 0.0) / 100.0

            market_fees = total_gross * market_tariff_pct
            broker_fees = total_gross * broker_tariff_pct

            total_commission = market_fees + broker_fees
            taxes = 0
            if commissions_data.get("commissionIva", False):
                taxes = total_commission * config.VAT_RATE

            calculated_fees = total_commission + taxes
            json_fees = abs(total_net - total_gross)
            if not np.isclose(calculated_fees, json_fees, atol=0.01):
                logging.warning(
                    f"Fee calculation discrepancy for tx {tx_id}. Calculated: {calculated_fees}, From JSON: {json_fees}"
                )

            ticker = (
                (tx.get("symbol") or instrument.get("name", "")).replace("D", "")
                if tx.get("currency") == "USD"
                else (tx.get("symbol") or instrument.get("name"))
            )
            price = (
                float(tx["shareValue"]) / 100.0
                if instrument.get("priceUnitScale") == 100
                else float(tx["shareValue"])
            )

            clean_tx = {
                "broker_id": tx_id,
                "date": pd.to_datetime(tx["operationDate"]).tz_localize(None),
                "op_type": tx["orderOperation"],
                "ticker": ticker,
                "asset_type": asset_type,
                "quantity": float(tx["executedAmount"]),
                "price": price,
                "currency": tx["currency"],
                "total_net": total_net,  # Guardamos el total neto para usarlo después
                "market_fees": market_fees,
                "broker_fees": broker_fees,
                "taxes": taxes,
            }
            if asset_type == "OPCION":
                details = parse_option_details(instrument.get("galloName", ""))
                details["expiration_date"] = pd.to_datetime(
                    instrument.get("maturityDate"), errors="coerce"
                )
                clean_tx.update(details)
            new_transactions.append(clean_tx)
            processed_ids.add(tx_id)
        except Exception as e:
            logging.warning(f"Could not process transaction {tx_id}: {e}")

    new_transactions.sort(key=lambda x: x["date"])
    return new_transactions


def _apply_sell_transaction(tx, open_positions, rates):
    """Applies a sell transaction against open lots."""
    newly_closed_trades = []
    remaining_to_sell = tx["quantity"]
    rate = rates.get_rate(tx["date"], tx["asset_type"])
    if not rate:
        logging.warning(f"No exchange rate for {tx['ticker']} on {tx['date'].date()}")

    # Usar el total neto de la venta
    revenue_ars = tx["total_net"] if tx["currency"] == "ARS" else tx["total_net"] * rate
    revenue_usd = revenue_ars / rate if rate else None

    matching_lots = sorted(
        [p for p in open_positions if p["ticker"] == tx["ticker"]],
        key=lambda p: p["date"],
    )

    for lot in matching_lots:
        if remaining_to_sell <= 0:
            break
        qty_from_lot = min(lot["quantity"], remaining_to_sell)
        proportion = qty_from_lot / lot["quantity"] if lot["quantity"] > 0 else 0

        closed_trade = {
            "ticker": lot["ticker"],
            "quantity": qty_from_lot,
            "buy_date": lot["date"],
            "sell_date": tx["date"],
            "total_cost_ars": (lot.get("total_cost_ars") or 0) * proportion,
            "total_cost_usd": (lot.get("total_cost_usd") or 0) * proportion,
            "total_revenue_ars": (revenue_ars or 0) * (qty_from_lot / tx["quantity"])
            if tx["quantity"] > 0
            else 0,
            "total_revenue_usd": (revenue_usd or 0) * (qty_from_lot / tx["quantity"])
            if tx["quantity"] > 0
            else 0,
            "buy_broker_transaction_id": lot.get("broker_id"),
            "sell_broker_transaction_id": tx["broker_id"],
        }
        newly_closed_trades.append(closed_trade)

        lot["quantity"] -= qty_from_lot
        if lot.get("total_cost_ars"):
            lot["total_cost_ars"] *= 1 - proportion
        if lot.get("total_cost_usd"):
            lot["total_cost_usd"] *= 1 - proportion
        remaining_to_sell -= qty_from_lot

    return newly_closed_trades


def _save_portfolio_state(open_positions, newly_closed_trades):
    # ... (sin cambios en esta función) ...
    """Saves the updated open positions and appends closed trades to CSV files."""
    open_df = pd.DataFrame(open_positions)
    open_df.rename(
        columns={
            "date": "purchase_date",
            "broker_id": "broker_transaction_id",
            "currency": "original_currency",
        },
        inplace=True,
    )

    final_cols = [
        "purchase_date",
        "ticker",
        "quantity",
        "total_cost_ars",
        "total_cost_usd",
        "asset_type",
        "original_currency",
        "lotes",
        "market_fees",
        "broker_fees",
        "taxes",
        "broker_transaction_id",
    ]

    for col in final_cols:
        if col not in open_df.columns:
            open_df[col] = pd.NA

    # Asegurarse de que no se guarden columnas extra como 'total_net'
    open_df = open_df.reindex(columns=final_cols)

    open_df.to_csv(config.OPEN_POSITIONS_FILE, index=False, date_format="%Y-%m-%d")

    if newly_closed_trades:
        new_closed_df = pd.DataFrame(newly_closed_trades)
        file_exists = (
            os.path.exists(config.CLOSED_TRADES_FILE)
            and os.path.getsize(config.CLOSED_TRADES_FILE) > 0
        )
        new_closed_df.to_csv(
            config.CLOSED_TRADES_FILE,
            mode="a",
            header=not file_exists,
            index=False,
            date_format="%Y-%m-%d",
        )


def reconcile_portfolio():
    """Main reconciliation script orchestrating the load, process, and save steps."""
    rates = ExchangeRateLoader()
    processed_ids = _load_processed_ids()
    new_transactions = _load_and_filter_new_transactions(processed_ids)

    try:
        open_positions = pd.read_csv(
            config.OPEN_POSITIONS_FILE, parse_dates=["purchase_date", "expiration_date"]
        ).to_dict("records")
    except (FileNotFoundError, pd.errors.EmptyDataError):
        open_positions = []

    newly_closed_trades = []
    for tx in new_transactions:
        if tx["op_type"] == "BUY":
            rate = rates.get_rate(tx["date"], tx["asset_type"])

            # Usar el total neto de la compra para el costo
            cost_ars = (
                tx["total_net"]
                if tx["currency"] == "ARS"
                else tx["total_net"] * rate
                if rate
                else None
            )
            cost_usd = cost_ars / rate if rate else None

            lot = tx.copy()
            lot.update({"total_cost_ars": cost_ars, "total_cost_usd": cost_usd})
            open_positions.append(lot)

        elif tx["op_type"] == "SELL":
            closed_from_tx = _apply_sell_transaction(tx, open_positions, rates)
            newly_closed_trades.extend(closed_from_tx)
            open_positions = [p for p in open_positions if p["quantity"] > 0.001]

    _save_portfolio_state(open_positions, newly_closed_trades)


if __name__ == "__main__":
    reconcile_portfolio()
