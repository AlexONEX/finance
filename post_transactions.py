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

    for i, tx in enumerate(transactions):
        tx_id = tx.get("id", "N/A")
        try:
            response = requests.post(API_URL, headers=headers, json=tx, timeout=15)

        except requests.exceptions.RequestException as e:
            print(f"Error al enviar la transacción {tx_id}: {e}")
            with open("failed_transactions.log", "a", encoding="utf-8") as log_file:
                log_file.write(f"Transacción {tx_id} fallida: {str(e)}\n")
            break

        time.sleep(0.1)

    print("\nProceso finalizado.")


if __name__ == "__main__":
    send_transactions()
