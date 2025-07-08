from flask import Flask, request, jsonify
import logging
from src.domain.portfolio import Portfolio
from src.shared.utils import map_instrument_to_asset_type, parse_option_details

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
app = Flask(__name__)
portfolio = Portfolio()


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
        "date_str": data["operationDate"].split("T")[0],
        "ticker": ticker,
        "currency": currency,
        "quantity": float(data["executedAmount"]),
        "price": price,
        "market_fees": abs(
            float(data.get("total", 0)) - float(data.get("totalGross", 0))
        ),
        "taxes": 0,
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
        positions = portfolio.get_open_positions()
        return jsonify({"status": "success", "data": positions}), 200
    except Exception as e:
        logging.error(f"Error retrieving open positions: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/positions/closed", methods=["GET"])
def get_closed_positions():
    """Endpoint to retrieve all closed positions."""
    try:
        positions = portfolio.get_closed_positions()
        return jsonify({"status": "success", "data": positions}), 200
    except Exception as e:
        logging.error(f"Error retrieving closed positions: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/transaction", methods=["POST"])
def add_transaction():
    """Endpoint to add a new transaction to the portfolio."""
    data = request.get_json()
    if not data:
        return jsonify({"status": "error", "message": "Invalid JSON"}), 400

    broker_id = data.get("id")
    if broker_id and portfolio.is_transaction_processed(broker_id):
        return jsonify(
            {
                "status": "skipped_duplicate",
                "message": f"Transaction ID {broker_id} already processed.",
            }
        ), 200

    op_type = data.get("orderOperation")
    if op_type not in ["BUY", "SELL"] or data.get("state") != "FULFILLED":
        return jsonify(
            {
                "status": "skipped",
                "message": f"Operation type '{op_type}' or state '{data.get('state')}' skipped.",
            }
        ), 200

    try:
        tx_data = parse_transaction_request(data)
    except (KeyError, TypeError, ValueError) as e:
        logging.error(f"Error parsing transaction data: {e} - Data: {data}")
        return jsonify({"status": "error", "message": f"Data parsing error: {e}"}), 400

    try:
        if op_type == "BUY":
            portfolio.record_buy(**tx_data)
        elif op_type == "SELL":
            # record_sell might have a different signature, e.g., quantity_to_sell
            tx_data["quantity_to_sell"] = tx_data.pop("quantity")
            portfolio.record_sell(**tx_data)

        msg = f"Successfully processed {op_type} for {tx_data['quantity_to_sell'] if op_type == 'SELL' else tx_data['quantity']} of {tx_data['ticker']}."
        logging.info(msg)
        return jsonify({"status": "success", "message": msg}), 201
    except Exception as e:
        logging.error(f"Error processing transaction in portfolio: {e}", exc_info=True)
        return jsonify(
            {"status": "error", "message": f"Portfolio processing error: {e}"}
        ), 500


if __name__ == "__main__":
    app.run(debug=True, port=5001)
