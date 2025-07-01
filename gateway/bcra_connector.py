import requests
import logging


class BCRAAPIConnector:
    """Manages the connection to the BCRA Statistics API."""

    BASE_URL = "https://api.bcra.gob.ar/estadisticas/v3.0/monetarias"
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            logging.info("Creating a new BCRAAPIConnector instance.")
            cls._instance = super(BCRAAPIConnector, cls).__new__(cls)
        return cls._instance

    def get_series_data(self, variable_id: int):
        """Fetches the complete data series for a specific variable ID."""
        url = f"{self.BASE_URL}/{variable_id}"
        logging.info(f"Contacting BCRA API for variable ID: {variable_id}")
        try:
            # verify=False is used as a workaround for local SSL environment issues.
            response = requests.get(url, timeout=15, verify=False)
            response.raise_for_status()
            data = response.json()
            return data.get("results", [])
        except requests.exceptions.HTTPError as e:
            logging.error(
                f"HTTP Error for ID {variable_id}: {e.response.status_code} {e.response.reason}"
            )
            return None
        except requests.exceptions.RequestException as e:
            logging.error(f"BCRA API connection error: {e}")
            return None
        except ValueError as e:
            logging.error(f"Error parsing JSON response for ID {variable_id}: {e}")
            return None
