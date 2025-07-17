import requests
import json
import time

API_URL = "http://127.0.0.1:5001/transaction"
TRANSACTIONS_FILE = "transactions.json"


def send_transactions():
    try:
        with open(TRANSACTIONS_FILE, "r", encoding="utf-8") as f:
            transactions = json.load(f)
    except FileNotFoundError:
        print(f"Error: El archivo '{TRANSACTIONS_FILE}' no fue encontrado.")
        return
    except json.JSONDecodeError:
        print(f"Error: El archivo '{TRANSACTIONS_FILE}' no es un JSON válido.")
        return

    headers = {"Content-Type": "application/json"}

    transactions.reverse()
    print(f"Se encontraron {len(transactions)} transacciones para procesar.")

    for i, tx in enumerate(transactions):
        tx_id = tx.get("id", "N/A")
        print(f"\n[{i + 1}/{len(transactions)}] Procesando transacción ID: {tx_id}...")

        try:
            response = requests.post(API_URL, headers=headers, json=tx, timeout=15)

            print(f"-> Api response: {response.status_code} - {response.reason}")

        except requests.exceptions.RequestException as e:
            print(f"!! Error de conexión al enviar la transacción {tx_id}: {e}")
            print("!! Asegúrate de que la API esté corriendo con 'python3 run_api.py'")
            break

        time.sleep(0.1)

    print("\nProceso finalizado.")


if __name__ == "__main__":
    send_transactions()
