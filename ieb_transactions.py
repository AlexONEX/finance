import requests
import json
from datetime import datetime
import config


def fetch_all_broker_transactions():
    """
    Fetches all transactions from the IEB broker API, saves the full unfiltered
    history, and also saves a clean, filtered version for processing.
    """
    try:
        start_date = datetime.strptime(config.STARTING_OPERATING_DATE, "%d-%m-%Y")
    except ValueError:
        print(
            "❌ Error: El formato de STARTING_OPERATING_DATE en config.py debe ser 'DD-MM-YYYY'."
        )
        return

    bearer_token = input(
        "🔑 Pega aquí el access_token del navegador y presiona Enter:\n"
    )
    if not bearer_token.strip():
        print("❌ Error: No se ingresó ningún token. El script se detendrá.")
        return

    end_date = datetime.now()
    start_date_str = start_date.strftime("%Y-%m-%dT03:00:00Z")
    end_date_str = end_date.strftime("%Y-%m-%dT02:59:59Z")
    base_url = (
        "https://core.iebmas.grupoieb.com.ar/api/orders/customer-account/26935110/page"
    )
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:140.0) Gecko/20100101 Firefox/140.0",
        "X-device-id": "131b36aa-d2cf-4759-961f-2ae196f0ff9d",
        "X-Client-Name": "WEB 0.30.1",
    }
    all_transactions = []
    current_page = 0

    print(
        f"\n🔄 Iniciando la descarga de transacciones desde {config.STARTING_OPERATING_DATE}..."
    )

    try:
        while True:
            params = {
                "page": current_page,
                "size": 50,
                "sort": "createdDate,desc",
                "operationDate.greaterThanOrEqual": start_date_str,
                "operationDate.lessThanOrEqual": end_date_str,
            }
            print(f"    📄 Obteniendo página {current_page}...")
            response = requests.get(
                base_url, headers=headers, params=params, timeout=30
            )
            response.raise_for_status()
            page_data = response.json()

            if not page_data:
                print("    ✅ No se encontraron más transacciones.")
                break

            all_transactions.extend(page_data)
            current_page += 1

        if all_transactions:
            full_history_file = "transactions_full_history.json"
            with open(full_history_file, "w", encoding="utf-8") as f:
                json.dump(all_transactions, f, ensure_ascii=False, indent=4)
            print(
                f"\n💾 Historial completo guardado en '{full_history_file}' ({len(all_transactions)} transacciones totales)."
            )

            # --- CORRECCIÓN DEL TYPO AQUÍ ---
            filtered_transactions = [
                tx
                for tx in all_transactions
                if tx.get("state") in ["FULFILLED", "PARTIALLY_FULLFILLED"]
            ]
            print(
                f"    👍 Filtrando... {len(filtered_transactions)} transacciones válidas encontradas."
            )

            filtered_transactions.reverse()  # Ordenar de más antigua a más nueva

            with open(config.TRANSACTIONS_FILE, "w", encoding="utf-8") as f:
                json.dump(filtered_transactions, f, ensure_ascii=False, indent=4)
            print(
                f"✅ Historial filtrado y listo para procesar guardado en '{config.TRANSACTIONS_FILE}'."
            )

    except Exception as e:
        print(f"\n❌ Ocurrió un error inesperado: {e}")


if __name__ == "__main__":
    fetch_all_broker_transactions()
