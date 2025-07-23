import requests
import logging
import pandas as pd

class AmbitoGateway:
    BASE_URL = "https://mercados.ambito.com"
    USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

    def fetch_historical_data(self, endpoint: str, start_date: str, end_date: str):
        """
        Fetches historical data from a specific Ambito endpoint.

        Args:
            endpoint: The API endpoint (e.g., "dolarrava/cl").
            start_date: Start date in "YYYY-MM-DD" format.
            end_date: End date in "YYYY-MM-DD" format.
            verify_ssl: Whether to verify the SSL certificate.

        Returns:
            A list of raw data rows or None if an error occurs.
        """
        url = f"{self.BASE_URL}/{endpoint}/historico-general/{start_date}/{end_date}"
        try:
            response = requests.get(url, headers={'User-Agent': self.USER_AGENT}, timeout=15, verify=True)
            response.raise_for_status()
            json_response = response.json()
            return json_response[1:] if len(json_response) > 1 else []
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching data from Ambito: {e}")
        except (ValueError, IndexError) as e:
            logging.error(f"Error parsing Ambito JSON response: {e}")

        return None

    def parse_historical_data(self, data_rows: list) -> pd.DataFrame:
        """
        Parses raw historical data into a clean pandas DataFrame.

        Args:
            data_rows: A list of lists, where each inner list is a row.

        Returns:
            A DataFrame with 'date' and 'value' columns.
        """
        if not data_rows:
            return pd.DataFrame(columns=['date', 'value'])

        df = pd.DataFrame(data_rows, columns=['date_str', 'value_str'])
        df['date'] = pd.to_datetime(df['date_str'], format='%d/%m/%Y')
        df['value'] = df['value_str'].str.replace(',', '.').astype(float)

        return df[['date', 'value']]
