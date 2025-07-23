import pandas as pd
import logging
import os
from datetime import datetime, timedelta
import urllib3

import config
from .gateways.bcra_gateway import BCRAAPIGateway
from .gateways.ambito_gateway import AmbitoGateway
from .gateways.alpha_vantage_gateway import AlphaVantageAPIGateway

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def _get_last_date_from_csv(file_path: str) -> pd.Timestamp | None:
    """Reads a CSV and returns the last date found, or None if empty/missing."""
    if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
        return None
    try:
        df = pd.read_csv(file_path, parse_dates=["date"])
        if df.empty or "date" not in df.columns:
            return None
        return df["date"].max()
    except Exception as e:
        logging.error(f"Could not read last date from {file_path}: {e}")
        return None


def _append_to_csv(file_path: str, new_data_df: pd.DataFrame):
    """Appends a DataFrame to a CSV file, creating it if it doesn't exist."""
    if new_data_df.empty:
        return

    file_exists = os.path.exists(file_path) and os.path.getsize(file_path) > 0
    new_data_df.to_csv(
        file_path,
        mode="a",
        header=not file_exists,
        index=False,
        date_format="%Y-%m-%d",
    )
    logging.info(
        f"Successfully appended {len(new_data_df)} new records to {os.path.basename(file_path)}."
    )


def update_dolar_ambito(file_path: str, endpoint: str, asset_name: str):
    """Efficiently updates historical dollar data from Ambito."""
    last_date = _get_last_date_from_csv(file_path)

    if last_date and last_date.date() >= datetime.now().date():
        logging.info(f"{asset_name} data is up to date. Skipping update.")
        return

    start_date_obj = (
        (last_date + timedelta(days=1))
        if last_date
        else datetime.strptime(config.STARTING_OPERATING_DATE, "%d-%m-%Y")
    )
    start_date = start_date_obj.strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")

    if start_date > end_date:
        logging.info(f"{asset_name} data is up to date. Skipping update.")
        return

    logging.info(f"Updating {asset_name} data from {start_date} to {end_date}...")
    gateway = AmbitoGateway()
    raw_data = gateway.fetch_historical_data(endpoint, start_date, end_date)
    if raw_data:
        df = gateway.parse_historical_data(raw_data)
        _append_to_csv(file_path, df)


def update_dolar_mep():
    update_dolar_ambito(
        config.DOLAR_MEP_FILE, config.AMBITO_DOLAR_MEP_ENDPOINT, "Dolar MEP"
    )


def update_dolar_ccl():
    update_dolar_ambito(
        config.DOLAR_CCL_FILE, config.AMBITO_DOLAR_CCL_ENDPOINT, "Dolar CCL"
    )


def _update_full_history_series(
    file_path: str, asset_name: str, fetch_func, date_key: str, value_key: str
):
    """
    Updates series that provide full history in each call (like BCRA, AlphaVantage).
    It overwrites the file to ensure data integrity.
    """
    # For these series, a daily check is sufficient.
    last_date = _get_last_date_from_csv(file_path)
    if last_date and last_date.date() >= (datetime.now() - timedelta(days=1)).date():
        logging.info(f"{asset_name} data is recent. Skipping update.")
        return

    logging.info(f"Updating {asset_name} data (full history)...")
    api_data = fetch_func()
    if not isinstance(api_data, list) or not api_data:
        logging.warning(f"No data received for {asset_name}. Skipping save.")
        return

    records = []
    for record in api_data:
        try:
            date_val = record.get(date_key)
            val = record.get(value_key)
            if date_val is None or val is None:
                continue
            record_date = pd.to_datetime(date_val).normalize()
            value = float(str(val).replace(",", "."))
            records.append({"date": record_date, "value": value})
        except (ValueError, TypeError, KeyError):
            continue

    if records:
        df = pd.DataFrame(records).drop_duplicates(subset="date").sort_values("date")
        df.to_csv(file_path, index=False, date_format="%Y-%m-%d")
        logging.info(
            f"Successfully saved full history for {os.path.basename(file_path)} with {len(df)} records."
        )


def update_cer():
    connector = BCRAAPIGateway()
    fetch_func = lambda: connector.get_series_data(
        variable_id=30, start_date="", end_date=""
    )
    _update_full_history_series(config.CER_FILE, "CER", fetch_func, "fecha", "valor")


def update_cpi_usa():
    connector = AlphaVantageAPIGateway()
    _update_full_history_series(
        config.CPI_USA_FILE, "USA CPI", connector.get_cpi_data, "date", "value"
    )
