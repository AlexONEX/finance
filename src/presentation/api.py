from flask import Flask, request, jsonify
import logging
import pandas as pd
import re
import os
import json
import config

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


def parse_transaction_request(data: dict) -> dict:
    """Parses and validates transaction data from an API request."""
    instrument = data.get("instrument")
    if not instrument:
        raise ValueError("Instrument data is missing.")

    asset_type = map_instrument_to_asset_type(instrument)
    if asset_type == "UNKNOWN":
        raise ValueError(f"Unknown asset type for instrument: {instrument}")

    ticker = data.get("symbol") or instrument.get("name")
    currency = data["currency"]
    price = float(data["shareValue"])

    if currency == "USD" and ticker and ticker.upper().endswith("D"):
        ticker = ticker[:-1]

    if asset_type == "OPCION":
        gallo_name = instrument.get("galloName", "")
        if gallo_name:
            sanitized_ticker = re.sub(r"[\s.,()]", "", gallo_name).upper()
            ticker = sanitized_ticker

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

    parsed = {
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
    }

    if parsed["quantity"] <= 0:
        raise ValueError("Transaction quantity must be positive.")

    if asset_type == "OPCION":
        option_details = parse_option_details(instrument.get("galloName", ""))
        option_details["expiration_date"] = instrument.get("maturityDate")
        parsed.update(option_details)

    return parsed


@app.route("/positions/open", methods=["GET"])
def get_open_positions():
    """Endpoint to retrieve all open positions."""
    try:
        repository = PortfolioRepository()
        portfolio = repository.load_full_portfolio()
        reporting_service = ReportingService(portfolio)
        report = reporting_service.generate_open_positions_report()
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
def get_closed_positions():
    """Endpoint to retrieve all closed positions."""
    try:
        repository = PortfolioRepository()
        portfolio = repository.load_full_portfolio()
        reporting_service = ReportingService(portfolio)
        report = reporting_service.generate_closed_trades_report()
        return jsonify(
            {"status": "success", "data": report.to_dict(orient="records")}
        ), 200
    except Exception as e:
        logging.error(f"Error retrieving closed positions: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500


def log_unresolved_transaction(data: dict, error: Exception):
    """Appends a failed transaction to the to_resolve.csv file."""
    file_exists = os.path.exists(config.TO_RESOLVE_FILE)
    with open(config.TO_RESOLVE_FILE, "a", newline="", encoding="utf-8") as f:
        f.write(
            json.dumps({"id": data.get("id"), "error": str(error), "data": data}) + "\n"
        )


@app.route("/transaction", methods=["POST"])
def add_transaction():
    data = request.get_json()
    if not data:
        return jsonify({"status": "error", "message": "Invalid JSON"}), 400

    valid_states = ["FULFILLED", "PARTIALLY_FULLFILLED"]
    op_type = data.get("orderOperation")

    if op_type not in ["BUY", "SELL"] or data.get("state") not in valid_states:
        return jsonify(
            {
                "status": "skipped",
                "message": f"Operation type '{op_type}' or state '{data.get('state')}' skipped.",
            }
        ), 200

    try:
        repository = PortfolioRepository()
        portfolio = repository.load_full_portfolio()
        broker_id = str(data.get("id"))
        all_ids = set()
        if (
            not portfolio.open_positions.empty
            and "broker_transaction_id" in portfolio.open_positions.columns
        ):
            all_ids.update(
                portfolio.open_positions["broker_transaction_id"].dropna().astype(str)
            )
        if not portfolio.closed_trades.empty:
            if "buy_broker_transaction_id" in portfolio.closed_trades.columns:
                all_ids.update(
                    portfolio.closed_trades["buy_broker_transaction_id"]
                    .dropna()
                    .astype(str)
                )
            if "sell_broker_transaction_id" in portfolio.closed_trades.columns:
                all_ids.update(
                    portfolio.closed_trades["sell_broker_transaction_id"]
                    .dropna()
                    .astype(str)
                )
        if broker_id in all_ids:
            return jsonify(
                {
                    "status": "skipped_duplicate",
                    "message": f"Transaction ID {broker_id} already processed.",
                }
            ), 200

        tx_data = parse_transaction_request(data)
        transaction_service = TransactionService(portfolio, repository)

        if op_type == "BUY":
            transaction_service.record_buy(tx_data)
        elif op_type == "SELL":
            transaction_service.record_sell(tx_data)

        return jsonify(
            {"status": "processed", "id": tx_data.get("broker_transaction_id")}
        ), 200

    except ValueError as e:
        logging.error(f"Logical error processing transaction {data.get('id')}: {e}")
        log_unresolved_transaction(data, e)
        return jsonify({"status": "conflict", "message": f"Logical conflict: {e}"}), 409

    except Exception as e:
        logging.error(f"Error processing transaction in portfolio: {e}", exc_info=True)
        return jsonify(
            {"status": "error", "message": f"Portfolio processing error: {e}"}
        ), 500


if __name__ == "__main__":
    app.run(debug=True, port=5001)
