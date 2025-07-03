import os
import requests
import logging


class AlphaVantageAPIGateway:
    BASE_URL = "https://www.alphavantage.co/query"

    def __init__(self, api_key: str = None):
        """
        Initializes the gateway.

        Args:
            api_key: The Alpha Vantage API key. If not provided, it will be
                     loaded from the 'ALPHA_VANTAGE_API_KEY' environment variable.

        Raises:
            ValueError: If the API key is not found.
        """
        from dotenv import load_dotenv

        load_dotenv()
        self.api_key = api_key or os.getenv("ALPHA_VANTAGE_API_KEY")
        if not self.api_key:
            raise ValueError("Alpha Vantage API key is not set or provided.")

    def _make_request(self, params: dict):
        """Helper function to perform API requests."""
        params["apikey"] = self.api_key
        try:
            response = requests.get(self.BASE_URL, params=params, timeout=15)
            response.raise_for_status()
            api_response = response.json()

            if note := api_response.get("Note"):
                logging.warning(f"Note from Alpha Vantage API: {note}")

            return api_response
        except requests.exceptions.RequestException as e:
            logging.error(f"Alpha Vantage API connection error: {e}")
        except ValueError as e:
            logging.error(f"Error parsing Alpha Vantage JSON response: {e}")

        return None

    def get_cpi_data(self):
        """Fetches the monthly US CPI data series."""
        logging.info("Contacting Alpha Vantage for CPI data...")
        params = {"function": "CPI", "interval": "monthly", "datatype": "json"}
        response = self._make_request(params)
        return response.get("data", []) if response else []

    def get_quote_endpoint(self, symbol: str):
        """Fetches real-time quote data for a given symbol."""
        logging.info(f"Contacting Alpha Vantage for quote on symbol: {symbol}")
        params = {"function": "GLOBAL_QUOTE", "symbol": symbol}
        response = self._make_request(params)

        if not response:
            return None

        global_quote = response.get("Global Quote", {})
        if not global_quote or "05. price" not in global_quote:
            logging.warning(f"No valid data returned for symbol {symbol}.")
            return None

        return {
            "price": float(global_quote["05. price"]),
            "change_percent": float(
                global_quote.get("10. change percent", "0%").replace("%", "")
            ),
        }
