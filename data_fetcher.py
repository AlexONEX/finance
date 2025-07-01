# data_fetcher.py
import os
import pandas as pd
from datetime import datetime
import logging
import warnings
import requests
from gateway import BCRAAPIConnector, AlphaVantageAPIConnector

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
        existing_dates = set()

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


def update_exchange_rates(file_path="data/exchange_rates.csv"):
    """Updates the exchange rate file using BCRAAPIConnector."""
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
