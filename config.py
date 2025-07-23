import os

# Magic numbers and constants
FALLBACK_MONTHLY_INFLATION_RATE = 0.002
VAT_RATE = 0.21  # 21% VAT

DATA_DIR = "data"

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

OPEN_POSITIONS_FILE = os.path.join(DATA_DIR, "open_positions.csv")
CLOSED_TRADES_FILE = os.path.join(DATA_DIR, "closed_trades.csv")

EXCHANGE_RATES_FILE = os.path.join(DATA_DIR, "exchange_rates.csv")
CER_FILE = os.path.join(DATA_DIR, "cer.csv")
CPI_USA_FILE = os.path.join(DATA_DIR, "cpi_usa.csv")
DOLAR_CCL_FILE = os.path.join(DATA_DIR, "dolar_ccl.csv")
DOLAR_MEP_FILE = os.path.join(DATA_DIR, "dolar_mep.csv")
RETAIL_DOLAR_FILE = os.path.join(DATA_DIR, "retail_dolar.csv")

BOND_PRICE_DIVISOR = 100
OPTION_LOT_SIZE = 100

STARTING_OPERATING_DATE = "20-01-2025"

AMBITO_BASE_URL = "https://mercados.ambito.com"
AMBITO_DOLAR_CCL_ENDPOINT = "dolarrava/cl"
AMBITO_DOLAR_MEP_ENDPOINT = "dolarrava/mep"
DATA912_API_URL = "https://data912.com"

IEB_ACCOUNT_ID = "26935110"
IEB_ORDERS_URL = f"https://core.iebmas.grupoieb.com.ar/api/orders/customer-account/{IEB_ACCOUNT_ID}/page"
IEB_DIVIDENDS_URL = f"https://core.iebmas.grupoieb.com.ar/api/portfolio/customer-account/{IEB_ACCOUNT_ID}/dividends"

TRANSACTIONS_FILE = "transactions.json"
