import pandas as pd
import logging
import os
from datetime import datetime, timedelta
import urllib3

import config
from .gateways.data912_connector import Data912APIConnector
from .gateways.bcra_gateway import BCRAAPIGateway
from .gateways.ambito_gateway import AmbitoGateway

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def is_file_up_to_date(file_path: str, max_days_old: int = 1) -> bool:
    if not os.path.exists(file_path):
        return False
    try:
        df = pd.read_csv(file_path, parse_dates=["date"])
        if df.empty:
            return False
        last_date = df["date"].iloc[-1].normalize()
        return last_date >= (datetime.now() - timedelta(days=max_days_old)).normalize()
    except Exception:
        return False


def _get_date_range():
    start_date = datetime.strptime(config.STARTING_OPERATING_DATE, "%d-%m-%Y").strftime(
        "%Y-%m-%d"
    )
    end_date = datetime.now().strftime("%Y-%m-%d")
    return start_date, end_date


def _save_full_history(file_path: str, data: list, date_key: str, value_key: str):
    if not isinstance(data, list) or not data:
        logging.warning(
            f"No API data or invalid format received to update {file_path}. Skipping."
        )
        return
    records = []
    for record in data:
        try:
            date_val = record.get(date_key)
            if date_val is None:
                continue
            record_date = pd.to_datetime(date_val).normalize()
            value = float(str(record[value_key]).replace(",", "."))
            records.append({"date": record_date, "value": value})
        except (ValueError, TypeError, KeyError):
            continue
    if records:
        df = pd.DataFrame(records)
        df.sort_values(by="date", inplace=True)
        df.drop_duplicates(subset="date", keep="last", inplace=True)
        df.to_csv(file_path, index=False, date_format="%Y-%m-%d")
        logging.info(
            f"Successfully saved full history for {os.path.basename(file_path)} with {len(df)} records."
        )


def update_cpi_usa():
    # El CPI de USA es mensual, la lógica de chequeo de fecha no es tan crítica pero se puede agregar
    if is_file_up_to_date(config.CPI_USA_FILE, max_days_old=25):
        logging.info("USA CPI data is recent. Skipping update.")
        return
    logging.info("Updating USA CPI data...")
    from .gateways.alpha_vantage_gateway import AlphaVantageAPIGateway

    connector = AlphaVantageAPIGateway()
    api_data = connector.get_cpi_data()
    if isinstance(api_data, list):
        _save_full_history(
            config.CPI_USA_FILE, api_data, date_key="date", value_key="value"
        )


def update_cer():
    if is_file_up_to_date(
        config.CER_FILE, max_days_old=3
    ):  # El BCRA puede demorar en actualizar
        logging.info("CER data is up to date. Skipping update.")
        return
    logging.info("Updating CER data...")
    start_date, end_date = _get_date_range()
    connector = BCRAAPIGateway()
    api_data = connector.get_series_data(
        variable_id=30, start_date=start_date, end_date=end_date
    )
    if isinstance(api_data, list):
        _save_full_history(
            config.CER_FILE, api_data, date_key="fecha", value_key="valor"
        )


def update_dolar_mep():
    if is_file_up_to_date(config.DOLAR_MEP_FILE):
        logging.info("Dolar MEP data is up to date. Skipping update.")
        return
    logging.info("Updating Dolar MEP historical data from Ambito...")
    start_date, end_date = _get_date_range()
    gateway = AmbitoGateway()
    raw_data = gateway.fetch_historical_data(
        config.AMBITO_DOLAR_MEP_ENDPOINT, start_date, end_date
    )
    if raw_data:
        df = gateway.parse_historical_data(raw_data)
        df.to_csv(config.DOLAR_MEP_FILE, index=False, date_format="%Y-%m-%d")
        logging.info(f"Successfully saved Dolar MEP history with {len(df)} records.")


def update_dolar_ccl():
    if is_file_up_to_date(config.DOLAR_CCL_FILE):
        logging.info("Dolar CCL data is up to date. Skipping update.")
        return
    logging.info("Updating Dolar CCL historical data from Ambito...")
    start_date, end_date = _get_date_range()
    gateway = AmbitoGateway()
    raw_data = gateway.fetch_historical_data(
        config.AMBITO_DOLAR_CCL_ENDPOINT, start_date, end_date
    )
    if raw_data:
        df = gateway.parse_historical_data(raw_data)
        df.to_csv(config.DOLAR_CCL_FILE, index=False, date_format="%Y-%m-%d")
        logging.info(f"Successfully saved Dolar CCL history with {len(df)} records.")


# El resto de las funciones (helpers e update_historical_asset) no cambian
def _read_existing_data(file_path):
    if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
        df = pd.read_csv(file_path, parse_dates=["date"])
        return df, set(df["date"])
    return pd.DataFrame(columns=["date", "value"]), set()


def _prepare_new_records(api_data, existing_dates, date_key, value_key):
    new_records = []
    for record in api_data:
        if not isinstance(record, dict):
            logging.warning(f"Skipping non-dict record in API data: {record}")
            continue
        try:
            date_val = record.get(date_key)
            if date_val is None:
                continue
            record_date = pd.to_datetime(date_val).normalize()
            if record_date not in existing_dates:
                value = float(str(record.get(value_key)).replace(",", "."))
                new_records.append({"date": record_date, "value": value})
        except (ValueError, TypeError, KeyError):
            continue
    return new_records


def update_csv_from_api(file_path: str, api_data: list, date_key: str, value_key: str):
    if not isinstance(api_data, list):
        logging.warning(
            f"Received non-list API data for {file_path}. Skipping update. Data: {api_data}"
        )
        return
    df, existing_dates = _read_existing_data(file_path)
    new_records = _prepare_new_records(api_data, existing_dates, date_key, value_key)
    if new_records:
        new_df = pd.DataFrame(new_records)
        combined_df = pd.concat([df, new_df], ignore_index=True)
        combined_df.sort_values(by="date", inplace=True)
        combined_df.to_csv(file_path, index=False, date_format="%Y-%m-%d")


def update_historical_asset(asset_type: str, ticker: str):
    connector = Data912APIConnector()
    api_data = None
    asset_type_lower = asset_type.lower()
    if asset_type_lower == "accion":
        api_data = connector.get_historical_stock(ticker)
    elif asset_type_lower == "cedear":
        api_data = connector.get_historical_cedear(ticker)
    elif asset_type_lower in ["rf", "bono", "letra", "on"]:
        api_data = connector.get_historical_bond(ticker)
    else:
        logging.info(
            f"Historical data fetch is not supported for asset type: {asset_type}"
        )
        return
    file_path = os.path.join(
        config.DATA_DIR, f"historical_{asset_type_lower}_{ticker}.csv"
    )
    if isinstance(api_data, list):
        update_csv_from_api(file_path, api_data, date_key="date", value_key="c")
