# src/infrastructure/gateways/bcra_gateway.py
import requests
import logging


class BCRAAPIGateway:
    """Manages the connection and data fetching from the BCRA Statistics API."""

    BASE_URL = "https://api.bcra.gob.ar/estadisticas/v3.0/monetarias"

    def get_series_data(
        self, variable_id: int, start_date: str, end_date: str, verify_ssl: bool = False
    ):
        """
        Fetches the complete data series for a specific variable ID.
        NOTE: start_date and end_date are ignored as the new endpoint gives full history.
        """
        url = f"{self.BASE_URL}/{variable_id}"

        try:
            # verify=False para evitar problemas de SSL que a veces tiene la API del BCRA.
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
