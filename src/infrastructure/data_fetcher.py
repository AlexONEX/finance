import pandas as pd
import logging
import requests
import os

import config
from .gateways.data912_connector import Data912APIConnector
from .gateways.bcra_gateway import (
    BCRAAPIGateway,
)
from .gateways.alpha_vantage_gateway import AlphaVantageAPIGateway
from datetime import datetime

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
            df["date"] = pd.to_datetime(df["date"]).dt.normalize()
            return df, set(df["date"])
    except (FileNotFoundError, pd.errors.EmptyDataError, Exception) as e:
        logging.warning(
            f"Could not read {file_path}, a new one will be created. Error: {e}"
        )
    return pd.DataFrame(columns=["date", "value"]), set()


def _prepare_new_records(
    api_data: list, existing_dates: set, date_key: str, value_key: str
) -> list:
    """Filters API data to find records not present in existing dates."""
    if not isinstance(api_data, list):
        logging.warning(
            f"API data is not a list, skipping record preparation. Data: {api_data}"
        )
        return []

    new_records = []
    for record in api_data:
        if not isinstance(record, dict):
            logging.warning(f"Skipping record because it is not a dictionary: {record}")
            continue

        try:
            record_date = pd.to_datetime(record.get(date_key)).normalize()
            if record_date not in existing_dates:
                value = float(str(record[value_key]).replace(",", "."))
                new_records.append({"date": record_date, "value": value})
        except (ValueError, TypeError, KeyError) as e:
            logging.debug(
                f"Skipping record due to format or key error: {record} - Error: {e}"
            )
            continue
    return new_records


def update_historical_asset(asset_type: str, ticker: str):
    """Checks for historical data updates for a given asset using Data912."""
    connector = Data912APIConnector()
    api_data = None

    asset_type_lower = asset_type.lower()

    if asset_type_lower == "accion":
        api_data = connector.get_historical_stock(ticker)
    elif asset_type_lower == "cedear":
        api_data = connector.get_historical_cedear(ticker)
    elif asset_type_lower in ["rf", "bono", "letra"]:
        api_data = connector.get_historical_bond(ticker)
    elif asset_type_lower == "opcion":
        logging.info(
            f"Historical data fetch is not supported for asset type: {asset_type}"
        )
        return
    else:
        logging.error(f"Unknown asset type for historical data: {asset_type}")
        return

    file_path = os.path.join(
        config.DATA_DIR, f"historical_{asset_type_lower}_{ticker}.csv"
    )
    update_csv_from_api(file_path, api_data, date_key="date", value_key="c")


def update_csv_from_api(file_path: str, api_data: list, date_key: str, value_key: str):
    """Updates a CSV file with new data from an API."""
    if api_data is None:
        logging.warning(f"No API data received to update {file_path}. Skipping.")
        return
    df, existing_dates = _read_existing_data(file_path)
    new_records = _prepare_new_records(api_data, existing_dates, date_key, value_key)
    if new_records:
        new_df = pd.DataFrame(new_records)
        combined_df = pd.concat([df, new_df], ignore_index=True)
        combined_df.sort_values(by="date", inplace=True)
        combined_df.to_csv(file_path, index=False, date_format="%Y-%m-%d")


def update_cpi_argentina():
    """Updates Argentina CPI file from BCRA."""
    connector = BCRAAPIGateway()
    api_data = connector.get_series_data(variable_id=28)
    update_csv_from_api(
        config.CPI_ARG_FILE, api_data, date_key="fecha", value_key="valor"
    )


def update_cpi_usa():
    """Updates USA CPI file from AlphaVantage."""
    connector = AlphaVantageAPIGateway()
    api_data = connector.get_cpi_data()
    update_csv_from_api(
        config.CPI_USA_FILE, api_data, date_key="date", value_key="value"
    )


def update_dolar_mep():
    """Updates Dolar MEP history by appending the latest value from Data912."""
    connector = Data912APIConnector()
    api_data = connector.get_mep()  # Fetches live data

    if api_data and isinstance(api_data, list) and len(api_data) > 0:
        # Assume the first instrument is the reference
        mep_price = api_data[0].get("mark")
        if mep_price:
            today_record = [{"date": datetime.now(), "value": mep_price}]
            update_csv_from_api(
                config.DOLAR_MEP_FILE,
                today_record,
                date_key="date",
                value_key="value",
            )
        else:
            logging.warning("Could not find 'mark' price in Data912 MEP response.")
    else:
        logging.warning("No data received from Data912 for Dolar MEP.")


def update_dolar_ccl():
    """Updates Dolar CCL history by appending the latest value from Data912."""
    connector = Data912APIConnector()
    api_data = connector.get_ccl()  # Fetches live data

    if api_data and isinstance(api_data, list) and len(api_data) > 0:
        # Assume the first instrument is the reference
        ccl_price = api_data[0].get("CCL_mark")
        if ccl_price:
            today_record = [{"date": datetime.now(), "value": ccl_price}]
            update_csv_from_api(
                config.DOLAR_CCL_FILE,
                today_record,
                date_key="date",
                value_key="value",
            )
        else:
            logging.warning("Could not find 'CCL_mark' price in Data912 CCL response.")
    else:
        logging.warning("No data received from Data912 for Dolar CCL.")
