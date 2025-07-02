from flask import Flask, request, jsonify
from portfolio import Portfolio  # Assuming Portfolio class is in portfolio.py
from utils import map_instrument_to_asset_type, parse_option_details
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
app = Flask(__name__)
portfolio = Portfolio()


@app.route("/positions/open", methods=["GET"])
def get_open_positions():
    """
    Retrieves all open positions from the portfolio.
    """
    try:
        open_positions = portfolio.get_open_positions()
        return jsonify({"status": "success", "data": open_positions}), 200
    except Exception as e:
        logging.error(f"Error retrieving open positions: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/positions/closed", methods=["GET"])
def get_closed_positions():
    """
    Retrieves all closed positions from the portfolio.
    """
    try:
        # This assumes your Portfolio class has a method to get closed positions.
        closed_positions = portfolio.get_closed_positions()
        return jsonify({"status": "success", "data": closed_positions}), 200
    except Exception as e:
        logging.error(f"Error retrieving closed positions: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/transaction", methods=["POST"])
def add_transaction():
    """
    Processes and adds a new transaction to the portfolio.
    """
    data = request.get_json()
    if not data:
        return jsonify({"status": "error", "message": "Invalid JSON"}), 400

    op_type = data.get("orderOperation")
    state = data.get("state")
    broker_id = data.get("id")

    if broker_id and portfolio.is_transaction_processed(broker_id):
        msg = f"Transaction ID {broker_id} already processed. Skipped."
        logging.info(msg)
        return jsonify({"status": "skipped_duplicate", "message": msg}), 200

    if op_type not in ["BUY", "SELL"] or state != "FULFILLED":
        msg = f"Operation type '{op_type}' or state '{state}' skipped."
        logging.info(msg)
        return jsonify({"status": "skipped", "message": msg}), 200

    try:
        instrument = data.get("instrument")
        if not instrument:
            raise ValueError("Instrument data is missing.")

        asset_type = map_instrument_to_asset_type(instrument)
        date_str = data["operationDate"].split("T")[0]
        ticker = data.get("symbol") or instrument.get("name")
        currency = data["currency"]
        quantity = float(data["executedAmount"])
        price = float(data["shareValue"])

        option_details = {}
        if asset_type == "OPCION":
            option_details = parse_option_details(instrument.get("galloName", ""))
            option_details["expiration_date"] = instrument.get("maturityDate")

        if instrument.get("priceUnitScale") == 100:
            price /= 100.0
        if currency == "USD" and ticker.upper().endswith("D"):
            ticker = ticker[:-1]

        total_gross = float(data.get("totalGross", 0))
        total_net = float(data.get("total", 0))
        fees_and_taxes = abs(total_net - total_gross)

        if asset_type == "UNKNOWN":
            raise ValueError(f"Unknown asset type: {instrument}")
        if quantity <= 0:
            raise ValueError("Transaction quantity must be positive.")

    except (KeyError, TypeError, ValueError) as e:
        logging.error(f"Error parsing transaction data: {e} - Data: {data}")
        return jsonify({"status": "error", "message": f"Data parsing error: {e}"}), 400

    try:
        common_args = {
            "broker_transaction_id": broker_id,
            "date_str": date_str,
            "ticker": ticker,
            "currency": currency,
            "price": price,
            "market_fees": fees_and_taxes,
            "taxes": 0,
            "asset_type": asset_type,
        }
        common_args.update(option_details)

        if op_type == "BUY":
            portfolio.record_buy(quantity=quantity, **common_args)
        elif op_type == "SELL":
            portfolio.record_sell(quantity_to_sell=quantity, **common_args)

        msg = f"Successfully processed {op_type} for {quantity} of {ticker}."
        logging.info(msg)
        return jsonify({"status": "success", "message": msg}), 201

    except Exception as e:
        logging.error(f"Error processing transaction in portfolio: {e}", exc_info=True)
        return jsonify(
            {"status": "error", "message": f"Portfolio processing error: {e}"}
        ), 500


# --- Main Execution ---
if __name__ == "__main__":
    # Use port 5001 to avoid potential conflicts with other services
    app.run(debug=True, port=5001)
