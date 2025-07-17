from flask import Flask, request, jsonify
import logging
import pandas as pd

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

    if instrument.get("priceUnitScale") == 100:
        price /= 100.0
    if currency == "USD" and ticker.upper().endswith("D"):
        ticker = ticker[:-1]

    parsed = {
        "broker_transaction_id": data.get("id"),
        "date": pd.to_datetime(data["operationDate"].split("T")[0]),
        "ticker": ticker,
        "currency": currency,
        "quantity": float(data["executedAmount"]),
        "price": price,
        # Se asume que las comisiones e impuestos se manejan en el servicio
        "market_fees": abs(
            float(data.get("total", 0)) - float(data.get("totalGross", 0))
        ),
        "broker_fees": 0,  # Placeholder, as it's not in the provided JSON structure
        "taxes": 0,  # Placeholder
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


@app.route("/transaction", methods=["POST"])
def add_transaction():
    data = request.get_json()
    if not data:
        return jsonify({"status": "error", "message": "Invalid JSON"}), 400

    op_type = data.get("orderOperation")
    if op_type not in ["BUY", "SELL"] or data.get("state") != "FULFILLED":
        return jsonify(
            {
                "status": "skipped",
                "message": f"Operation type '{op_type}' or state '{data.get('state')}' skipped.",
            }
        ), 200

    try:
        instrument_data = data.get("instrument", {})
        # Usamos la función existente para determinar el tipo de activo
        asset_type = map_instrument_to_asset_type(instrument_data)

        if asset_type == "OPCION":
            logging.info(f"Ignoring option transaction: {data.get('id')}")
            return jsonify(
                {
                    "status": "skipped_option",
                    "id": data.get("id"),
                    "message": "Transaction was ignored because it is an option.",
                }
            ), 200
    except Exception as e:
        # Esto es una salvaguarda en caso de que la data del instrumento sea extraña
        logging.warning(f"Could not determine asset type for tx {data.get('id')}: {e}")

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

    except (KeyError, TypeError, ValueError) as e:
        logging.error(f"Error parsing transaction data: {e} - Data: {data}")
        return jsonify({"status": "error", "message": f"Data parsing error: {e}"}), 400

    except Exception as e:
        logging.error(f"Error processing transaction in portfolio: {e}", exc_info=True)
        return jsonify(
            {"status": "error", "message": f"Portfolio processing error: {e}"}
        ), 500


if __name__ == "__main__":
    app.run(debug=True, port=5001)
