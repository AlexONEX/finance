import requests
import json
from datetime import datetime, timedelta

end_date = datetime.now()
start_date = end_date - timedelta(days=180)
start_date_str = start_date.strftime("%Y-%m-%dT03:00:00Z")
end_date_str = end_date.strftime("%Y-%m-%dT02:59:59Z")

BEARER_TOKEN = input(
    "🔑 Pega aquí el access_token que copiaste del navegador y presiona Enter:\n"
)

if not BEARER_TOKEN.strip():
    print("❌ Error: No se ingresó ningún token. El script se detendrá.")
else:
    base_url = (
        "https://core.iebmas.grupoieb.com.ar/api/orders/customer-account/26935110/page"
    )

    headers = {
        "Authorization": f"Bearer {BEARER_TOKEN}",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:140.0) Gecko/20100101 Firefox/140.0",
        "X-device-id": "131b36aa-d2cf-4759-961f-2ae196f0ff9d",
        "X-Client-Name": "WEB 0.30.1",
    }

    all_transactions = []
    current_page = 0

    print("\n🔄 Iniciando la descarga de transacciones con el access_token...")

    try:
        while True:
            params = {
                "page": current_page,
                "size": 100,
                "sort": "createdDate,desc",
                "operationDate.greaterThanOrEqual": start_date_str,
                "operationDate.lessThanOrEqual": end_date_str,
                "state.in": "FULFILLED",
            }

            print(f"    📄 Obteniendo página {current_page}...")
            response = requests.get(
                base_url, headers=headers, params=params, timeout=20
            )

            if response.status_code == 401:
                print(
                    "\n🚨 ¡Error 401: No autorizado! El access_token ha expirado o es inválido."
                )
                break

            response.raise_for_status()
            page_data = response.json()

            if not page_data:
                print("    ✅ No se encontraron más transacciones.")
                break

            all_transactions.extend(page_data)
            current_page += 1

        if all_transactions:
            file_name = "transactions.json"
            all_transactions.sort(
                key=lambda x: x.get("operationDate", ""), reverse=True
            )
            with open(file_name, "w", encoding="utf-8") as f:
                json.dump(all_transactions, f, ensure_ascii=False, indent=4)
            print(
                f"\n🎉 ¡Éxito! Se guardaron {len(all_transactions)} transacciones en '{file_name}'."
            )

    except Exception as e:
        print(f"\n❌ Ocurrió un error inesperado: {e}")
