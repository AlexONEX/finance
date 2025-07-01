# data_fetcher.py
import os
import pandas as pd
from datetime import datetime, date
import logging
import warnings
import requests
from gateway import BCRAAPIConnector, AlphaVantageAPIConnector
from services.exchange_rate_service import ExchangeRateService

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
warnings.simplefilter(action="ignore", category=FutureWarning)

requests.packages.urllib3.disable_warnings(
    requests.packages.urllib3.exceptions.InsecureRequestWarning
)


def _update_csv_with_api_data(
    file_path: str, api_data: list, date_format: str, value_key: str
):
    """Generic function to update a CSV file with data from an API."""
    if api_data is None:
        logging.warning(f"No API data received to update {file_path}. Skipping.")
        return

    print(f"DEBUG: _update_csv_with_api_data - file_path: {file_path}")
    print(f"DEBUG: _update_csv_with_api_data - api_data length: {len(api_data)}")
    if api_data:
        first_api_date = datetime.strptime(api_data[0].get("fecha") or api_data[0].get("date"), date_format)
        last_api_date = datetime.strptime(api_data[-1].get("fecha") or api_data[-1].get("date"), date_format)
        print(f"DEBUG: _update_csv_with_api_data - API data date range: {first_api_date.strftime('%Y-%m-%d')} to {last_api_date.strftime('%Y-%m-%d')}")

    existing_dates = set()
    try:
        df = (
            pd.read_csv(file_path)
            if os.path.exists(file_path)
            else pd.DataFrame(columns=["date", "value"])
        )
        df["date"] = pd.to_datetime(df["date"])
        existing_dates = set(df["date"])
    except Exception as e:
        logging.warning(
            f"Could not read {file_path}, a new one will be created. Error: {e}"
        )
        df = pd.DataFrame(columns=["date", "value"])

    print(f"DEBUG: _update_csv_with_api_data - existing_dates length: {len(existing_dates)}")
    if existing_dates:
        print(f"DEBUG: _update_csv_with_api_data - existing_dates date range: {min(existing_dates).strftime('%Y-%m-%d')} to {max(existing_dates).strftime('%Y-%m-%d')}")

    new_records = []
    for record in api_data:
        try:
            date_str = record.get("fecha") or record.get("date")
            if not date_str:
                continue

            date = datetime.strptime(date_str, date_format)
            if date not in existing_dates:
                value = float(str(record[value_key]).replace(",", "."))
                new_records.append({"date": date, "value": value})
        except (ValueError, TypeError, KeyError) as e:
            logging.debug(
                f"Skipping record due to format or key error: {record} - Error: {e}"
            )
            continue

    if new_records:
        new_df = pd.DataFrame(new_records)
        df = pd.concat([df, new_df], ignore_index=True) if not df.empty else new_df
        df.sort_values(by="date", inplace=True)
        df.to_csv(file_path, index=False)
        logging.info(f"Added {len(new_records)} new records to {file_path}.")
    else:
        logging.info(f"No new data to add to {file_path}.")


def update_retail_exchange_rates(file_path="data/retail_dolar.csv"):
    """Updates the retail exchange rate file using BCRAAPIConnector."""
    logging.info("Starting Retail Exchange Rate update...")
    connector = BCRAAPIConnector()
    api_data = connector.get_series_data(variable_id=4)  # ID 4 for Retail Dollar
    _update_csv_with_api_data(
        file_path, api_data, date_format="%Y-%m-%d", value_key="valor"
    )


def update_cpi_argentina(file_path="data/cpi_argentina.csv"):
    """Updates the Argentina CPI file using BCRAAPIConnector."""
    logging.info("Starting ARS National CPI update...")
    connector = BCRAAPIConnector()
    api_data = connector.get_series_data(variable_id=28)  # ID 28 for Monthly Inflation
    _update_csv_with_api_data(
        file_path, api_data, date_format="%Y-%m-%d", value_key="valor"
    )


def update_cpi_usa_from_api(file_path="data/cpi_usa.csv"):
    """Updates the USA CPI file using AlphaVantageAPIConnector."""
    logging.info("Starting USA CPI update...")
    connector = AlphaVantageAPIConnector()
    api_data = connector.get_cpi_data()
    _update_csv_with_api_data(
        file_path, api_data, date_format="%Y-%m-%d", value_key="value"
    )

def update_all_exchange_rates():
    logging.info("Starting all exchange rates update...")
    exchange_rate_service = ExchangeRateService()
    today = date.today().strftime("%Y-%m-%d")
    # Fetch data from a reasonable start date, e.g., beginning of last year or a fixed date
    start_date_ambito = "2023-01-01"

    exchange_rate_service.update_dolar_ccl(start_date_ambito, today)
    exchange_rate_service.update_dolar_mep(start_date_ambito, today)
    update_retail_exchange_rates()
    logging.info("All exchange rates update finished.")

