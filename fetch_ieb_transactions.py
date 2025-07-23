import requests
import json
import pandas as pd
from datetime import datetime
import config


def fetch_all_broker_transactions():
    """
    Fetches orders and dividends from the IEB API, transforms dividends into
    order-like objects, and saves a unified list sorted by date.
    """
    try:
        start_date = datetime.strptime(config.STARTING_OPERATING_DATE, "%d-%m-%Y")
    except ValueError:
        print("Error: Invalid STARTING_OPERATING_DATE format in config.py.")
        return

    bearer_token = input("Paste browser access_token:\n")
    if not bearer_token.strip():
        print("Error: No token provided.")
        return

    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:140.0) Gecko/20100101 Firefox/140.0",
        "X-device-id": "a6babab7-d633-4e6f-b90e-44d4994a6673",
        "X-Client-Name": "WEB 0.30.1",
        "Origin": "https://hb.iebmas.com.ar",
        "Referer": "https://hb.iebmas.com.ar/",
    }
    end_date = datetime.now()
    print(
        "Fetching orders between {} and {}...".format(
            start_date.strftime("%d-%m-%Y"), end_date.strftime("%d-%m-%Y")
        )
    )
    all_orders = []
    current_page = 0
    while True:
        params = {
            "page": current_page,
            "size": 50,
            "sort": "createdDate,desc",
            "operationDate.greaterThanOrEqual": start_date.strftime(
                "%Y-%m-%dT03:00:00Z"
            ),
            "operationDate.lessThanOrEqual": end_date.strftime("%Y-%m-%dT02:59:59Z"),
        }
        response = requests.get(
            config.IEB_ORDERS_URL, headers=headers, params=params, timeout=30
        )
        response.raise_for_status()
        page_data = response.json()
        if not page_data:
            break
        all_orders.extend(page_data)
        current_page += 1
    print(f"  > Found {len(all_orders)} orders.")

    instrument_map = {
        order["instrument"]["symbol"]: order["instrument"]
        for order in all_orders
        if order.get("instrument") and order["instrument"].get("symbol")
    }

    print("Fetching dividends...")
    dividends_params = {
        "fromDate": start_date.strftime("%Y-%m-%dT03:00:00.000Z"),
        "toDate": end_date.strftime("%Y-%m-%dT02:59:59.999Z"),
    }
    response = requests.get(
        config.IEB_DIVIDENDS_URL, headers=headers, params=dividends_params, timeout=30
    )
    response.raise_for_status()
    dividends_data = response.json()
    print(f"  > Found {len(dividends_data)} dividends.")

    transformed_dividends = []
    for div in dividends_data:
        currency_map = {"DOLARUSA": "USD", "PESOS": "ARS"}
        ticker = div.get("ticker")
        amortization_amount = div.get("amortizationAmount", 0)
        earning_amount = div.get("earningAmount", 0)
        costs = abs(div.get("costs", 0))

        # Si hay amortización, se trata como una VENTA (vencimiento)
        if amortization_amount > 0:
            quantity = div.get("portfolioAmount")
            total_revenue = earning_amount + amortization_amount
            price = (total_revenue / quantity) * 100 if quantity > 0 else 0
            transformed_op = {
                "id": div.get("documentKey"),
                "operationDate": div.get("date"),
                "state": "FULFILLED",
                "orderOperation": "SELL",
                "instrument": None,  # Dejamos el instrumento como nulo por ahora
                "symbol": ticker,
                "currency": currency_map.get(div.get("currency"), "ARS"),
                "executedAmount": quantity,
                "shareValue": price,
                "totalGross": total_revenue,
                "total": total_revenue - costs,
                "commissions": {
                    "commissionIva": False,
                    "marketTariffPercentage": 0,
                    "tariffPercentage": 0,
                },
                "costs": costs,
            }
        # Si no, es un dividendo normal
        else:
            is_stock_dividend = div.get("currency") == ticker
            transformed_op = {
                "id": div.get("documentKey"),
                "operationDate": div.get("date"),
                "state": "FULFILLED",
                "orderOperation": "DIVIDEND_STOCK"
                if is_stock_dividend
                else "DIVIDEND_CASH",
                "instrument": instrument_map.get(
                    ticker
                ),  # Aquí podemos intentar un primer lookup
                "symbol": ticker,
                "currency": currency_map.get(div.get("currency"), "ARS"),
                "executedAmount": earning_amount if is_stock_dividend else 1,
                "shareValue": 0 if is_stock_dividend else earning_amount,
                "totalGross": earning_amount,
                "total": earning_amount - costs,
                "commissions": {"total_fees": costs},
            }
        transformed_dividends.append(transformed_op)

    all_movements = all_orders + transformed_dividends

    if all_movements:
        print("\nProcessing and saving all movements...")

        final_instrument_map = {
            mov["symbol"]: mov["instrument"]
            for mov in all_movements
            if mov.get("instrument") and mov.get("symbol")
        }

        for mov in all_movements:
            if not mov.get("instrument") and mov.get("symbol"):
                mov["instrument"] = final_instrument_map.get(mov["symbol"])

        valid_movements = [
            mov
            for mov in all_movements
            if (mov.get("orderOperation") in ["DIVIDEND_CASH", "DIVIDEND_STOCK"])
            or (mov.get("state") in ["FULFILLED", "PARTIALLY_FULLFILLED"])
        ]

        def get_date(x):
            date_str = x.get("operationDate", "")
            dt = (
                pd.to_datetime(date_str, dayfirst=True, errors="coerce")
                if "/" in date_str
                else pd.to_datetime(date_str, errors="coerce")
            )
            if pd.notna(dt):
                return dt.tz_localize(None)
            return dt

        valid_movements = [m for m in valid_movements if pd.notna(get_date(m))]
        valid_movements.sort(key=get_date)

        with open(config.TRANSACTIONS_FILE, "w", encoding="utf-8") as f:
            json.dump(valid_movements, f, ensure_ascii=False, indent=4)
        print(
            f"Saved {len(valid_movements)} valid movements to '{config.TRANSACTIONS_FILE}'."
        )


if __name__ == "__main__":
    fetch_all_broker_transactions()
