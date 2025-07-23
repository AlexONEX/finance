from flask import Flask, request, jsonify, g
from functools import wraps
import logging
import pandas as pd
import re

from src.shared.types import TransactionData
from src.infrastructure.persistence.portfolio_repository import PortfolioRepository
from src.application.transaction_service import TransactionService
from src.application.reporting_service import ReportingService
from src.shared.financial_utils import (
    map_instrument_to_asset_type,
    parse_option_details,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
app = Flask(__name__)


def inject_services(f):
    """Decorator to load portfolio and create services for a request."""

    @wraps(f)
    def decorated_function(*args, **kwargs):
        g.repository = PortfolioRepository()
        g.portfolio = g.repository.load_full_portfolio()
        g.transaction_service = TransactionService(g.portfolio, g.repository)
        g.reporting_service = ReportingService(g.portfolio)
        return f(*args, **kwargs)

    return decorated_function


def check_duplicate(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        data = request.get_json()
        broker_id = str(data.get("id"))
        portfolio = g.portfolio
        processed_ids = set()
        if (
            not portfolio.open_positions.empty
            and "broker_transaction_id" in portfolio.open_positions.columns
        ):
            ids = portfolio.open_positions["broker_transaction_id"].dropna().astype(str)
            processed_ids.update(ids)
        if not portfolio.closed_trades.empty:
            if "buy_broker_transaction_id" in portfolio.closed_trades.columns:
                ids = (
                    portfolio.closed_trades["buy_broker_transaction_id"]
                    .dropna()
                    .astype(str)
                )
                processed_ids.update(ids)
            if "sell_broker_transaction_id" in portfolio.closed_trades.columns:
                ids = (
                    portfolio.closed_trades["sell_broker_transaction_id"]
                    .dropna()
                    .astype(str)
                )
                processed_ids.update(ids)
        if broker_id in processed_ids:
            return jsonify(
                {
                    "status": "skipped_duplicate",
                    "message": f"Transaction ID {broker_id} already processed.",
                }
            ), 200
        return f(*args, **kwargs)

    return decorated_function


def parse_transaction_request(data: dict) -> dict:
    instrument = data.get("instrument")
    if not instrument:
        raise ValueError("Instrument data is missing.")
    asset_type = map_instrument_to_asset_type(instrument)
    if asset_type == "UNKNOWN":
        raise ValueError(f"Unknown asset type for instrument: {instrument}")
    ticker = data.get("symbol")
    currency = data["currency"]
    price = float(data["shareValue"])
    if instrument.get("priceUnitScale") == 100:
        price = price / 100.0
    if currency == "USD" and ticker and ticker.upper().endswith("D"):
        ticker = ticker[:-1]
    if asset_type == "OPCION":
        gallo_name = instrument.get("galloName")
        if not gallo_name:
            raise ValueError(
                f"Option transaction {data.get('id')} is missing galloName."
            )
        ticker = re.sub(r"[\s.,()]", "", gallo_name).upper()
    if not ticker:
        ticker = instrument.get("name")
    if not ticker:
        raise ValueError(
            f"Could not determine a valid ticker for transaction {data.get('id')}."
        )
    total_gross = float(data.get("totalGross", 0))
    commissions_data = data.get("commissions", {})
    market_tariff_pct = commissions_data.get("marketTariffPercentage", 0.0) / 100.0
    broker_tariff_pct = commissions_data.get("tariffPercentage", 0.0) / 100.0
    market_fees = abs(total_gross * market_tariff_pct)
    broker_fees = abs(total_gross * broker_tariff_pct)
    total_commission = market_fees + broker_fees
    taxes = (
        abs(total_commission * 0.21)
        if commissions_data.get("commissionIva", False)
        else 0
    )
    instrument_type_from_broker = instrument.get("type", "").upper()
    final_instrument_type = (
        asset_type
        if instrument_type_from_broker == "NONE"
        else instrument_type_from_broker
    )
    parsed: TransactionData = {
        "broker_transaction_id": data.get("id"),
        "date": pd.to_datetime(data["operationDate"].split("T")[0]),
        "ticker": ticker,
        "currency": currency,
        "quantity": float(data["executedAmount"]),
        "price": price,
        "market_fees": market_fees,
        "broker_fees": broker_fees,
        "taxes": taxes,
        "asset_type": asset_type,
        "instrument_type": final_instrument_type,
    }
    if parsed["quantity"] <= 0:
        raise ValueError("Transaction quantity must be positive.")
    if asset_type == "OPCION":
        option_details = parse_option_details(instrument.get("galloName", ""))
        parsed.update(option_details)
    if maturity_date := instrument.get("maturityDate"):
        parsed["expiration_date"] = pd.to_datetime(maturity_date)
    return parsed


@app.route("/positions/open", methods=["GET"])
@inject_services
def get_open_positions():
    try:
        report = g.reporting_service.generate_open_positions_report()
        consolidated_json = report["consolidated"].to_dict(orient="records")
        options_json = report["options"].to_dict(orient="records")
        return jsonify(
            {
                "status": "success",
                "data": {"consolidated": consolidated_json, "options": options_json},
            }
        ), 200
    except Exception as e:
        logging.error(f"Error retrieving open positions: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/positions/closed", methods=["GET"])
@inject_services
def get_closed_positions():
    try:
        report = g.reporting_service.generate_closed_trades_report()
        return jsonify(
            {"status": "success", "data": report.to_dict(orient="records")}
        ), 200
    except Exception as e:
        logging.error(f"Error retrieving closed positions: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/transaction", methods=["POST"])
@inject_services
@check_duplicate
def add_transaction():
    data = request.get_json()
    if not data:
        return jsonify({"status": "error", "message": "Invalid JSON"}), 400
    valid_states = ["FULFILLED", "PARTIALLY_FULLFILLED"]
    op_type = data.get("orderOperation")
    allowed_ops = ["BUY", "SELL", "DIVIDEND_STOCK"]
    if op_type not in allowed_ops or data.get("state") not in valid_states:
        return jsonify(
            {
                "status": "skipped",
                "message": f"Operation type '{op_type}' or state '{data.get('state')}' skipped.",
            }
        ), 200
    try:
        tx_data = parse_transaction_request(data)
        if op_type == "BUY" or op_type == "DIVIDEND_STOCK":
            g.transaction_service.record_buy(tx_data)
        elif op_type == "SELL":
            g.transaction_service.record_sell(tx_data)
        return jsonify(
            {"status": "processed", "id": tx_data.get("broker_transaction_id")}
        ), 200
    except Exception as e:
        tx_id = data.get("id", "N/A")
        op_type = data.get("orderOperation", "N/A")
        error_msg = f"Error processing transaction ID {tx_id} (Type: {op_type}): {e}"
        logging.error(error_msg, exc_info=True)
        return jsonify({"status": "error", "message": error_msg}), 500


@app.route("/maintenance/run", methods=["POST"])
@inject_services
def run_maintenance():
    """Endpoint to explicitly trigger maintenance tasks like expiring options."""
    try:
        g.transaction_service.expire_options()
        return jsonify(
            {"status": "success", "message": "Maintenance tasks completed."}
        ), 200
    except Exception as e:
        logging.error(f"Error during manual maintenance run: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True, port=5001)
