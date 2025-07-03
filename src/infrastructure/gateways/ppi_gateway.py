import os
import json
import logging
import threading
from dotenv import load_dotenv
from ppi_client.ppi import PPI
from ppi_client.models.instrument import Instrument


class PPIGateway:
    """
    Manages a single, persistent connection to PPI for real-time market data.
    This class is implemented as a Singleton.
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super(PPIGateway, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        """
        Initializes the gateway, but the connection is started separately.
        """
        if hasattr(self, "_initialized"):
            return

        with self._lock:
            if hasattr(self, "_initialized"):
                return

            self._initialized = True
            load_dotenv()
            self._price_cache = {}
            self._subscribed_tickers = set()
            self.client = PPI(sandbox=False)

            public_key = os.getenv("PPI_PUBLIC_KEY")
            private_key = os.getenv("PPI_PRIVATE_KEY")

            if not public_key or not private_key:
                raise ValueError(
                    "PPI_PUBLIC_KEY and PPI_PRIVATE_KEY must be set in .env file."
                )

            logging.info("Logging into PPI API...")
            self.client.account.login_api(public_key, private_key)
            logging.info("PPI Login successful.")

            # Start the real-time connection in a background thread
            rt_thread = threading.Thread(
                target=self._start_realtime_connection, daemon=True
            )
            rt_thread.start()

    def _start_realtime_connection(self):
        """Connects to the market data stream and starts listening."""
        logging.info("Starting real-time market data connection thread...")
        self.client.realtime.connect_to_market_data(
            on_connect=self._on_connect,
            on_disconnect=self._on_disconnect,
            on_message=self._on_market_data,
        )
        self.client.realtime.start_connections()

    def _on_connect(self):
        """Callback executed on successful WebSocket connection."""
        logging.info("Successfully connected to PPI real-time market data.")
        # We can re-subscribe to tickers if needed upon reconnection
        for ticker in list(self._subscribed_tickers):
            self._subscribe_to_instrument(ticker)

    def _on_disconnect(self):
        """Callback executed on disconnection."""
        logging.warning("Disconnected from PPI real-time market data.")

    def _on_market_data(self, data):
        """Callback executed when a new market data message is received."""
        try:
            msg = json.loads(data)
            # We are interested in trades to get the last price
            if msg.get("Trade"):
                ticker = msg.get("Ticker")
                price = msg.get("Price")
                if ticker and price is not None:
                    with self._lock:
                        self._price_cache[ticker] = float(price)
                    logging.debug(f"Price cache updated for {ticker}: {price}")
        except Exception as e:
            logging.error(f"Error processing market data message: {e}")

    def _subscribe_to_instrument(self, ticker: str):
        """Subscribes to a given instrument ticker."""
        # This is a simplified subscription logic.
        # We assume most stocks/cedears are 'ACCIONES' and bonds are 'BONOS'.
        # This might need refinement based on the specific asset.
        asset_class = "BONOS" if "GD" in ticker or "AL" in ticker else "ACCIONES"

        try:
            logging.info(f"Subscribing to real-time updates for {ticker}")
            self.client.realtime.subscribe_to_element(
                Instrument(ticker, asset_class, "INMEDIATA")
            )
            self._subscribed_tickers.add(ticker)
        except Exception as e:
            logging.error(f"Failed to subscribe to {ticker}: {e}")

    def get_current_price(self, ticker: str) -> float | None:
        """
        Gets the current price for a ticker from the real-time cache.
        If not in cache, it attempts an initial fetch and subscribes for real-time updates.
        """
        with self._lock:
            price = self._price_cache.get(ticker)

        # If the price is already cached, return it immediately
        if price is not None:
            return price

        # If not cached, try a direct fetch and subscribe for future updates
        logging.info(
            f"'{ticker}' not in cache. Fetching initial price and subscribing."
        )
        try:
            asset_class = "BONOS" if "GD" in ticker or "AL" in ticker else "ACCIONES"
            msg = self.client.marketdata.current(ticker, asset_class, "INMEDIATA")

            if msg and "price" in msg:
                initial_price = float(msg["price"])
                with self._lock:
                    self._price_cache[ticker] = initial_price

                # Subscribe for subsequent updates
                self._subscribe_to_instrument(ticker)
                return initial_price
            else:
                logging.warning(f"Could not fetch initial price for {ticker}.")
                return None
        except Exception as e:
            logging.error(f"Error on initial fetch for {ticker}: {e}")
            return None
