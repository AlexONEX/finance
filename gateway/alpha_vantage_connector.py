import os
import requests
import logging


class AlphaVantageAPIConnector:
    """Manages the connection to the Alpha Vantage API."""

    BASE_URL = "https://www.alphavantage.co/query"
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            logging.info("Creating a new AlphaVantageAPIConnector instance.")
            cls._instance = super(AlphaVantageAPIConnector, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        """Initializes the connector by loading the API key from environment variables."""
        # Asumiendo que la API key está en un archivo .env
        from dotenv import load_dotenv

        load_dotenv()
        self.api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
        if not self.api_key:
            logging.warning("Environment variable ALPHA_VANTAGE_API_KEY is not set.")

    def get_cpi_data(self):
        """Fetches the monthly CPI data series."""
        if not self.api_key:
            logging.error("Cannot contact Alpha Vantage without an API key.")
            return None

        logging.info("Contacting Alpha Vantage for CPI data...")
        params = {
            "function": "CPI",
            "interval": "monthly",
            "datatype": "json",
            "apikey": self.api_key,
        }
        try:
            response = requests.get(self.BASE_URL, params=params, timeout=15)
            response.raise_for_status()
            api_response = response.json()

            note = api_response.get("Note")
            if note:
                logging.warning(f"Note from Alpha Vantage API: {note}")

            return api_response.get("data", [])
        except requests.exceptions.RequestException as e:
            logging.error(f"Alpha Vantage API connection error: {e}")
            return None
        except ValueError as e:
            logging.error(f"Error parsing Alpha Vantage JSON response: {e}")
            return None

    def get_quote_endpoint(self, symbol: str):
        """Fetches real-time quote data for a given symbol."""
        if not self.api_key:
            logging.error("Cannot contact Alpha Vantage without an API key.")
            return None

        logging.info(f"Contacting Alpha Vantage for quote on symbol: {symbol}")
        params = {
            "function": "GLOBAL_QUOTE",
            "symbol": symbol,
            "apikey": self.api_key,
        }
        try:
            response = requests.get(self.BASE_URL, params=params, timeout=15)
            response.raise_for_status()
            data = response.json().get("Global Quote", {})

            # Check if the response contains a note (e.g., API limit reached)
            note = response.json().get("Note")
            if note:
                logging.warning(
                    f"Note from Alpha Vantage API for symbol {symbol}: {note}"
                )
                return None

            if not data:
                logging.warning(f"No data returned for symbol {symbol}.")
                return None

            # Check for empty price, which can happen for invalid tickers
            price_str = data.get("05. price")
            if price_str is None:
                logging.warning(f"Price not found for symbol {symbol}.")
                return None

            return {
                "price": float(price_str),
                "change_percent": float(
                    data.get("10. change percent", "0%").replace("%", "")
                ),
            }
        except (requests.exceptions.RequestException, ValueError, KeyError) as e:
            logging.error(f"Error fetching or parsing quote for {symbol}: {e}")
            return None
