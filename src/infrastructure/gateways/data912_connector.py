import requests
import logging
import config
from functools import lru_cache
import os

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


class Data912APIConnector:
    def __init__(self, timeout: int = 15):
        self.base_url = config.DATA912_API_URL
        self.timeout = timeout
        logging.info(f"Conector inicializado para la URL base: {self.base_url}")

    @lru_cache(maxsize=16)
    def _make_request(self, endpoint: str):
        """
        Método auxiliar para realizar peticiones GET a la API.

        Args:
            endpoint (str): El endpoint específico al que se va a llamar (ej. '/live/mep').

        Returns:
            Una lista o diccionario con los datos, o None si ocurre un error.
        """
        url = f"{self.base_url}{endpoint}"
        logging.info(f"Contactando API en el endpoint: {endpoint}")
        try:
            response = requests.get(url, timeout=self.timeout, verify=True)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            logging.error(
                f"Error HTTP para el endpoint {endpoint}: {e.response.status_code} {e.response.reason}"
            )
        except requests.exceptions.RequestException as e:
            logging.error(f"Error de conexión con la API en {url}: {e}")
        except (
            ValueError
        ) as e:
            logging.error(f"Error al decodificar la respuesta JSON desde {url}: {e}")

        return None

    def get_mep(self):
        return self._make_request("/live/mep")

    def get_ccl(self):
        return self._make_request("/live/ccl")

    def get_arg_stocks(self):
        return self._make_request("/live/arg_stocks")

    def get_arg_options(self):
        return self._make_request("/live/arg_options")

    def get_arg_cedears(self):
        return self._make_request("/live/arg_cedears")

    def get_arg_bonds(self):
        return self._make_request("/live/arg_bonds")

    def get_arg_notes(self):
        return self._make_request("/live/arg_notes")

    def get_arg_corporate_debt(self):
        return self._make_request("/live/arg_corp")

    def get_usa_adrs(self):
        return self._make_request("/live/usa_adrs")

    def get_usa_stocks(self):
        return self._make_request("/live/usa_stocks")

    def get_historical_stock(self, ticker: str):
        return self._make_request(f"/historical/stocks/{ticker.upper()}")

    def get_historical_cedear(self, ticker: str):
        return self._make_request(f"/historical/cedears/{ticker.upper()}")

    def get_historical_bond(self, ticker: str):
        return self._make_request(f"/historical/bonds/{ticker.upper()}")

    def get_volatilities(self, ticker: str):
        return self._make_request(f"/eod/volatilities/{ticker.upper()}")

    def get_option_chain(self, ticker: str):
        return self._make_request(f"/eod/option_chain/{ticker.upper()}")
