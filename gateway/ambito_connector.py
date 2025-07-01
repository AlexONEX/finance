import requests
import json
import logging
from datetime import datetime
import pandas as pd

class AmbitoConnector:
    BASE_URL = "https://mercados.ambito.com"

    def fetch_exchange_rate_history(self, rate_type: str, start_date: str, end_date: str) -> dict:
        """
        Fetches historical exchange rate data from Ambito Financiero.

        Args:
            rate_type (str): Type of exchange rate (e.g., "dolarrava/cl" for CCL, "dolarrava/mep" for MEP).
            start_date (str): Start date in "YYYY-MM-DD" format.
            end_date (str): End date in "YYYY-MM-DD" format.

        Returns:
            dict: JSON response from the API.
        """
        url = f"{self.BASE_URL}/{rate_type}/historico-general/{start_date}/{end_date}"
        logging.info(f"Fetching data from: {url}")
        try:
            response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}, timeout=15, verify=False) # verify=False for -k in curl
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching data from Ambito Financiero: {e}")
            return {}

    def parse_ambito_response(self, json_data: list) -> pd.DataFrame:
        """
        Parses the JSON response from Ambito Financiero into a pandas DataFrame.

        Args:
            json_data (list): List of lists from Ambito API response.

        Returns:
            pd.DataFrame: DataFrame with 'date' and 'value' columns.
        """
        if not json_data or len(json_data) < 2:
            return pd.DataFrame(columns=['date', 'value'])

        # First row is headers, subsequent rows are data
        data_rows = json_data[1:]
        df = pd.DataFrame(data_rows, columns=['date_str', 'value_str'])
        
        # Convert date and value to appropriate types
        df['date'] = pd.to_datetime(df['date_str'], format='%d/%m/%Y')
        df['value'] = df['value_str'].str.replace(',', '.').astype(float)
        
        return df[['date', 'value']]
