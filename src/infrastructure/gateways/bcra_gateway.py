import requests
import logging


class BCRAAPIGateway:
    """Manages the connection and data fetching from the BCRA Statistics API."""

    BASE_URL = "https://api.bcra.gob.ar/estadisticas/v3.0/monetarias"

    def get_series_data(self, variable_id: int, verify_ssl: bool = False):
        """
        Fetches the complete data series for a specific variable ID.

        Args:
            variable_id: The ID of the variable to fetch.
            verify_ssl: Whether to verify the SSL certificate. Defaults to False.

        Returns:
            A list of data points or None if an error occurs.
        """
        url = f"{self.BASE_URL}/{variable_id}"
        logging.info(f"Contacting BCRA API for variable ID: {variable_id}")
        try:
            response = requests.get(url, timeout=15, verify=verify_ssl)
            response.raise_for_status()
            data = response.json()
            return data.get("results", [])
        except requests.exceptions.HTTPError as e:
            logging.error(
                f"HTTP Error for ID {variable_id}: {e.response.status_code} {e.response.reason}"
            )
        except requests.exceptions.RequestException as e:
            logging.error(f"BCRA API connection error: {e}")
        except ValueError as e:
            logging.error(f"Error parsing JSON response for ID {variable_id}: {e}")

        return None
