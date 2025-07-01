import pandas as pd
from gateway.ambito_connector import AmbitoConnector
import os
import logging
from config import DOLAR_CCL_FILE, DOLAR_MEP_FILE, AMBITO_DOLAR_CCL_ENDPOINT, AMBITO_DOLAR_MEP_ENDPOINT

class ExchangeRateService:
    def __init__(self):
        self.ambito_connector = AmbitoConnector()

    def _load_data(self, file_path: str) -> pd.DataFrame:
        if os.path.exists(file_path):
            return pd.read_csv(file_path, parse_dates=['date'])
        return pd.DataFrame(columns=['date', 'value'])

    def _save_data(self, df: pd.DataFrame, file_path: str):
        df.to_csv(file_path, index=False)

    def update_dolar_ccl(self, start_date: str, end_date: str):
        logging.info(f"Updating Dolar CCL data from {start_date} to {end_date}")
        json_data = self.ambito_connector.fetch_exchange_rate_history(AMBITO_DOLAR_CCL_ENDPOINT, start_date, end_date)
        new_data_df = self.ambito_connector.parse_ambito_response(json_data)

        if not new_data_df.empty:
            existing_data_df = self._load_data(DOLAR_CCL_FILE)
            combined_df = pd.concat([existing_data_df, new_data_df]).drop_duplicates(subset=['date']).sort_values(by='date').reset_index(drop=True)
            self._save_data(combined_df, DOLAR_CCL_FILE)
            logging.info(f"Dolar CCL data updated and saved to {DOLAR_CCL_FILE}")
        else:
            logging.warning("No new Dolar CCL data fetched.")

    def update_dolar_mep(self, start_date: str, end_date: str):
        logging.info(f"Updating Dolar MEP data from {start_date} to {end_date}")
        json_data = self.ambito_connector.fetch_exchange_rate_history(AMBITO_DOLAR_MEP_ENDPOINT, start_date, end_date)
        new_data_df = self.ambito_connector.parse_ambito_response(json_data)

        if not new_data_df.empty:
            existing_data_df = self._load_data(DOLAR_MEP_FILE)
            combined_df = pd.concat([existing_data_df, new_data_df]).drop_duplicates(subset=['date']).sort_values(by='date').reset_index(drop=True)
            self._save_data(combined_df, DOLAR_MEP_FILE)
            logging.info(f"Dolar MEP data updated and saved to {DOLAR_MEP_FILE}")
        else:
            logging.warning("No new Dolar MEP data fetched.")
