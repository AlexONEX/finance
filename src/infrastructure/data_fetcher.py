import os
import pandas as pd
from datetime import date
import logging
import requests
from gateway import BCRAAPIConnector, AlphaVantageAPIConnector
from services.exchange_rate_service import ExchangeRateService
import config

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
requests.packages.urllib3.disable_warnings(
    requests.packages.urllib3.exceptions.InsecureRequestWarning
)


def _read_existing_data(file_path: str) -> tuple[pd.DataFrame, set]:
    """Reads a CSV file and returns its data and a set of existing dates."""
    try:
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            df = pd.read_csv(file_path)
            df["date"] = pd.to_datetime(df["date"])
            return df, set(df["date"])
    except (FileNotFoundError, pd.errors.EmptyDataError, Exception) as e:
        logging.warning(
            f"Could not read {file_path}, a new one will be created. Error: {e}"
        )

    return pd.DataFrame(columns=["date", "value"]), set()


def _prepare_new_records(
    api_data: list, existing_dates: set, date_key: str, value_key: str, date_format: str
) -> list:
    """Filters API data to find records not present in existing dates."""
    new_records = []
    if not api_data:
        return []

    for record in api_data:
        try:
            date_str = record.get(date_key)
            if not date_str:
                continue

            record_date = pd.to_datetime(date_str, format=date_format)
            if record_date not in existing_dates:
                value = float(str(record[value_key]).replace(",", "."))
                new_records.append({"date": record_date, "value": value})
        except (ValueError, TypeError, KeyError) as e:
            logging.debug(
                f"Skipping record due to format or key error: {record} - Error: {e}"
            )
            continue
    return new_records


def update_csv_from_api(
    file_path: str, api_data: list, date_key: str, value_key: str, date_format: str
):
    """Updates a CSV file with new data from an API."""
    if api_data is None:
        logging.warning(f"No API data received to update {file_path}. Skipping.")
        return

    df, existing_dates = _read_existing_data(file_path)
    new_records = _prepare_new_records(
        api_data, existing_dates, date_key, value_key, date_format
    )

    if new_records:
        new_df = pd.DataFrame(new_records)
        combined_df = pd.concat([df, new_df], ignore_index=True)
        combined_df.sort_values(by="date", inplace=True)
        combined_df.to_csv(file_path, index=False, date_format="%Y-%m-%d")
        logging.info(f"Added {len(new_records)} new records to {file_path}.")
    else:
        logging.info(f"No new data to add to {file_path}.")


def update_retail_exchange_rates():
    """Updates the retail exchange rate file from the BCRA API."""
    logging.info("Starting Retail Exchange Rate update...")
    connector = BCRAAPIConnector()
    api_data = connector.get_series_data(variable_id=4)
    update_csv_from_api(
        config.RETAIL_DOLAR_FILE,
        api_data,
        date_key="fecha",
        value_key="valor",
        date_format="%Y-%m-%d",
    )


def update_cpi_argentina():
    """Updates the Argentina CPI file from the BCRA API."""
    logging.info("Starting ARS National CPI update...")
    connector = BCRAAPIConnector()
    api_data = connector.get_series_data(variable_id=28)
    update_csv_from_api(
        config.CPI_ARG_FILE,
        api_data,
        date_key="fecha",
        value_key="valor",
        date_format="%Y-%m-%d",
    )


def update_cpi_usa_from_api():
    """Updates the USA CPI file from the AlphaVantage API."""
    logging.info("Starting USA CPI update...")
    connector = AlphaVantageAPIConnector()
    api_data = connector.get_cpi_data()
    update_csv_from_api(
        config.CPI_USA_FILE,
        api_data,
        date_key="date",
        value_key="value",
        date_format="%Y-%m-%d",
    )


def update_all_data():
    """Runs all data update processes."""
    logging.info("Starting all data updates...")
    exchange_rate_service = ExchangeRateService()
    today_str = date.today().strftime("%Y-%m-%d")
    start_date_str = "2023-01-01"

    exchange_rate_service.update_dolar_ccl(start_date_str, today_str)
    exchange_rate_service.update_dolar_mep(start_date_str, today_str)
    update_retail_exchange_rates()
    update_cpi_argentina()
    update_cpi_usa_from_api()
    logging.info("All data updates finished.")
