# config.py

DATA_DIR = "data"

# File paths for persisted data
OPEN_POSITIONS_FILE = f"{DATA_DIR}/open_positions.csv"
CLOSED_TRADES_FILE = f"{DATA_DIR}/closed_trades.csv"
EXCHANGE_RATES_FILE = f"{DATA_DIR}/exchange_rates.csv"
CPI_ARG_FILE = f"{DATA_DIR}/cpi_argentina.csv"
CPI_USA_FILE = f"{DATA_DIR}/cpi_usa.csv"
DOLAR_CCL_FILE = f"{DATA_DIR}/dolar_ccl.csv"
DOLAR_MEP_FILE = f"{DATA_DIR}/dolar_mep.csv"

# Ambito Financiero API
AMBITO_BASE_URL = "https://mercados.ambito.com"
AMBITO_DOLAR_CCL_ENDPOINT = "dolarrava/cl"
AMBITO_DOLAR_MEP_ENDPOINT = "dolarrava/mep"
