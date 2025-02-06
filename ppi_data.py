from decimal import Decimal
import os
from dotenv import load_dotenv
from ppi_client.ppi import PPI
from config import CEDEAR_POSITIONS, BOND_POSITIONS

class PPIData:
    def __init__(self):
        load_dotenv()
        self.ppi = PPI(sandbox=False)
        self.ppi.account.login_api(
            os.getenv('public_key'),
            os.getenv('private_key')
        )

    def get_cedear_prices(self):
        prices = {}
        for ticker in CEDEAR_POSITIONS.keys():
            try:
                data = self.ppi.marketdata.current(ticker, "CEDEARS", "INMEDIATA")
                prices[ticker] = Decimal(str(data['price'])) if data else None
            except Exception as e:
                print(f"Error getting CEDEAR price for {ticker}: {str(e)}")
                prices[ticker] = None
        return prices

    def get_bond_prices(self):
        prices = {}
        for ticker in BOND_POSITIONS.keys():
            try:
                data = self.ppi.marketdata.current(ticker, "BONOS", "INMEDIATA")
                prices[ticker] = Decimal(str(data['price'])) if data else None
            except Exception as e:
                print(f"Error getting bond price for {ticker}: {str(e)}")
                prices[ticker] = None
        return prices