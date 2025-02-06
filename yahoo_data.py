from decimal import Decimal
import yfinance as yf
import numpy as np
from config import CEDEAR_POSITIONS

class YahooData:
    def get_stock_prices(self):
        prices = {}
        for ticker, position in CEDEAR_POSITIONS.items():
            underlying = ticker.replace('D', '')  # Remove 'D' suffix
            try:
                stock = yf.Ticker(underlying)
                price = stock.history(period='1d')['Close'].iloc[-1]
                prices[underlying] = Decimal(str(price)) if not np.isnan(price) else None
            except Exception as e:
                print(f"Error getting stock price for {underlying}: {str(e)}")
                prices[underlying] = None
        return prices