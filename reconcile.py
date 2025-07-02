
# reconcile.py
import json
import pandas as pd
import logging
import re
from datetime import datetime
import os # Importamos os para chequear si el archivo existe

# --- CONFIGURACIÓN ---
TRANSACTIONS_FILE = "transactions.json"
OPEN_POSITIONS_FILE = "data/open_positions.csv"
CLOSED_TRADES_FILE = "data/closed_trades.csv"
DOLAR_MEP_FILE = "data/dolar_mep.csv"
DOLAR_CCL_FILE = "data/dolar_ccl.csv"

# --- LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- LÓGICA DE PARSING Y CLASIFICACIÓN (Sin cambios) ---
def map_instrument_to_asset_type(instrument: dict) -> str:
    """Clasifica un instrumento en un tipo de activo conocido."""
    if not instrument: return "UNKNOWN"
    instrument_type = instrument.get("type", "").upper()
    op_type = instrument.get("instrumentOperationType", "").upper()
    if op_type == "OPTION": return "OPCION"
    if instrument_type == "CEDEAR": return "CEDEAR"
    if instrument_type in ["MERVAL", "GENERAL", "LIDER", "PRIVATE_TITLE"]: return "ACCION"
    if instrument_type in ["BOND", "LETTER", "PUBLIC_TITLE"]: return "RF"
    if op_type == "PUBLIC_TITLE": return "RF"
    if op_type == "PRIVATE_TITLE": return "ACCION"
    return "UNKNOWN"

def parse_option_details(gallo_name: str) -> dict:
    """Extrae los detalles de un contrato de opción desde su nombre."""
    if not gallo_name: return {}
    match = re.match(r"([A-Z0-9]+)\s*\((C|V)\)\s*([\d,\.]+)", gallo_name.replace(".", ""))
    if not match: return {}
    return {
        "underlying_asset": match.group(1),
        "option_type": "CALL" if match.group(2) == "C" else "PUT",
        "strike_price": float(match.group(3).replace(",", "."))
    }

# --- LÓGICA DE DATOS (Sin cambios) ---
class ExchangeRateLoader:
    """Carga y provee los tipos de cambio desde los archivos CSV."""
    def __init__(self):
        try:
            self.dolar_mep = pd.read_csv(DOLAR_MEP_FILE, parse_dates=["date"])
        except FileNotFoundError: self.dolar_mep = pd.DataFrame(columns=["date", "value"])
        try:
            self.dolar_ccl = pd.read_csv(DOLAR_CCL_FILE, parse_dates=["date"])
        except FileNotFoundError: self.dolar_ccl = pd.DataFrame(columns=["date", "value"])

    def get_rate(self, date, asset_type: str):
        rate_df = self.dolar_mep if asset_type in ["ACCION", "RF", "OPCION"] else self.dolar_ccl
        if rate_df.empty: return None
        merged = pd.merge_asof(pd.DataFrame({"date": [pd.to_datetime(date)]}), rate_df.sort_values("date"), on="date", direction="nearest")
        return merged["value"].iloc[0] if not merged.empty and not pd.isna(merged["value"].iloc[0]) else None

# --- SCRIPT PRINCIPAL (MODIFICADO PARA SER INCREMENTAL Y MÁS ROBUSTO) ---
def reconcile_portfolio():
    logging.info(f"Starting incremental reconciliation from {TRANSACTIONS_FILE}...")
    
    rates = ExchangeRateLoader()
    
    # 1. Cargar IDs de transacciones existentes para evitar reprocesamiento
    processed_ids = set()
    try:
        open_pos_df = pd.read_csv(OPEN_POSITIONS_FILE)
        if 'broker_transaction_id' in open_pos_df.columns:
            processed_ids.update(open_pos_df['broker_transaction_id'].dropna().astype(str))
    except (FileNotFoundError, pd.errors.EmptyDataError):
        logging.info(f"{OPEN_POSITIONS_FILE} not found or is empty. Assuming no existing open positions.")
    except Exception as e:
        logging.error(f"Error reading {OPEN_POSITIONS_FILE} for IDs: {e}")

    try:
        closed_trades_df = pd.read_csv(CLOSED_TRADES_FILE)
        if 'buy_broker_transaction_id' in closed_trades_df.columns:
            processed_ids.update(closed_trades_df['buy_broker_transaction_id'].dropna().astype(str))
        if 'sell_broker_transaction_id' in closed_trades_df.columns:
            processed_ids.update(closed_trades_df['sell_broker_transaction_id'].dropna().astype(str))
    except (FileNotFoundError, pd.errors.EmptyDataError):
        logging.info(f"{CLOSED_TRADES_FILE} not found or is empty. Assuming no existing closed trades.")
    except Exception as e:
        logging.error(f"An unexpected error occurred while reading {CLOSED_TRADES_FILE}: {e}")

    # 2. Cargar y filtrar transacciones de origen
    try:
        with open(TRANSACTIONS_FILE, 'r', encoding='utf-8') as f:
            all_transactions = json.load(f)
    except Exception as e:
        logging.error(f"Could not read or parse {TRANSACTIONS_FILE}: {e}")
        return

    # Pre-procesar y filtrar para obtener solo transacciones NUEVAS
    new_transactions = []
    for tx in all_transactions:
        tx_id = str(tx.get("id"))
        if tx.get("state") != "FULFILLED" or tx.get("orderOperation") not in ["BUY", "SELL"]:
            continue
        if tx_id not in processed_ids:
            try:
                instrument = tx.get("instrument")
                if not instrument: continue
                
                asset_type = map_instrument_to_asset_type(instrument)
                if asset_type == "UNKNOWN": continue

                ticker, currency = tx.get("symbol") or instrument.get("name"), tx.get("currency")
                if currency == "USD" and ticker.upper().endswith("D"): ticker = ticker[:-1]
                
                price = float(tx["shareValue"])
                if instrument.get("priceUnitScale") == 100: price /= 100.0

                clean_tx = {
                    "broker_id": tx_id, "date": pd.to_datetime(tx["operationDate"]).tz_localize(None),
                    "op_type": tx["orderOperation"], "ticker": ticker, "asset_type": asset_type,
                    "quantity": float(tx["executedAmount"]), "price": price, "currency": currency,
                    "fees": abs(float(tx.get("total", 0)) - float(tx.get("totalGross", 0)))
                }
                if asset_type == "OPCION":
                    details = parse_option_details(instrument.get("galloName", ""))
                    details["expiration_date"] = pd.to_datetime(instrument.get("maturityDate"), errors='coerce')
                    clean_tx.update(details)
                new_transactions.append(clean_tx)
                processed_ids.add(tx_id) # Añadir al set para manejar duplicados dentro del mismo JSON
            except Exception as e:
                logging.warning(f"Could not process new transaction {tx.get('id')}: {e}")

    if not new_transactions:
        logging.info("No new transactions to process.")
        return

    # 3. Ordenar transacciones nuevas y cargar el estado actual del portafolio
    new_transactions.sort(key=lambda x: x['date'])
    logging.info(f"Processing {len(new_transactions)} new FULFILLED BUY/SELL transactions...")

    open_positions = []
    try:
        # Leer el CSV sin parsear fechas inicialmente para evitar errores si faltan columnas
        open_positions_df = pd.read_csv(OPEN_POSITIONS_FILE)
        
        # Columnas de fecha que esperamos y que podrían existir
        date_cols = ['purchase_date', 'expiration_date']
        for col in date_cols:
            if col in open_positions_df.columns:
                # Convertir la columna a datetime si existe, manejando valores nulos/malformados
                open_positions_df[col] = pd.to_datetime(open_positions_df[col], errors='coerce')

        open_positions_df.rename(columns={
            'purchase_date': 'date', 
            'broker_transaction_id': 'broker_id',
            'original_price': 'price',
            'original_currency': 'currency'
        }, inplace=True)
        open_positions = open_positions_df.to_dict('records')
    except (FileNotFoundError, pd.errors.EmptyDataError):
        open_positions = []
    except Exception as e:
        logging.error(f"Failed to load or process {OPEN_POSITIONS_FILE}: {e}")
        open_positions = []

    # 4. Procesar transacciones nuevas contra el estado actual
    newly_closed_trades = []
    for tx in new_transactions:
        rate = rates.get_rate(tx["date"], tx["asset_type"])
        if not rate: logging.warning(f"No exchange rate for {tx['ticker']} on {tx['date'].date()}")

        if tx["op_type"] == "BUY":
            cost_ars, cost_usd = None, None
            if tx["currency"] == "ARS":
                cost_ars = (tx["quantity"] * tx["price"]) + tx["fees"]
                if rate: cost_usd = cost_ars / rate
            elif tx["currency"] == "USD" and rate:
                cost_usd = (tx["quantity"] * tx["price"]) + (tx["fees"] / rate)
                cost_ars = cost_usd * rate
            
            lot = tx.copy()
            lot.update({"total_cost_ars": cost_ars, "total_cost_usd": cost_usd})
            open_positions.append(lot)
        
        elif tx["op_type"] == "SELL":
            remaining_to_sell = tx["quantity"]
            matching_lots = sorted([p for p in open_positions if p["ticker"] == tx["ticker"]], key=lambda p: p['date'])
            
            revenue_ars, revenue_usd = None, None
            if tx["currency"] == "ARS":
                revenue_ars = (tx["quantity"] * tx["price"]) - tx["fees"]
                if rate: revenue_usd = revenue_ars / rate
            elif tx["currency"] == "USD" and rate:
                revenue_usd = (tx["quantity"] * tx["price"]) - (tx["fees"] / rate)
                revenue_ars = revenue_usd * rate

            for lot in matching_lots:
                if remaining_to_sell <= 0: break
                qty_from_lot = min(lot["quantity"], remaining_to_sell)
                proportion = qty_from_lot / lot["quantity"] if lot["quantity"] > 0 else 0
                
                newly_closed_trades.append({
                    "ticker": lot["ticker"], "quantity": qty_from_lot, 
                    "buy_date": lot["date"],
                    "buy_price": lot.get("price"),
                    "buy_currency": lot.get("currency"),
                    "sell_date": tx["date"],
                    "sell_price": tx.get("price"),
                    "sell_currency": tx.get("currency"),
                    "total_cost_ars": (lot.get("total_cost_ars") or 0) * proportion,
                    "total_cost_usd": (lot.get("total_cost_usd") or 0) * proportion,
                    "total_revenue_ars": (revenue_ars or 0) * (qty_from_lot / tx["quantity"]) if tx["quantity"] > 0 else 0,
                    "total_revenue_usd": (revenue_usd or 0) * (qty_from_lot / tx["quantity"]) if tx["quantity"] > 0 else 0,
                    "buy_broker_transaction_id": lot.get("broker_id"), 
                    "sell_broker_transaction_id": tx["broker_id"]
                })
                lot["quantity"] -= qty_from_lot
                if lot.get("total_cost_ars"): lot["total_cost_ars"] *= (1 - proportion)
                if lot.get("total_cost_usd"): lot["total_cost_usd"] *= (1 - proportion)
                remaining_to_sell -= qty_from_lot
            open_positions = [p for p in open_positions if p["quantity"] > 0.001]

    # 5. Guardar resultados
    # Sobreescribir posiciones abiertas con el estado actualizado
    open_df = pd.DataFrame(open_positions).rename(columns={
        "date": "purchase_date", 
        "broker_id": "broker_transaction_id",
        "price": "original_price",
        "currency": "original_currency"
        })
    final_open_cols = ["purchase_date", "ticker", "quantity", "total_cost_ars", "total_cost_usd", "asset_type", "original_price", "original_currency", "underlying_asset", "option_type", "strike_price", "expiration_date", "broker_transaction_id"]
    for col in final_open_cols:
        if col not in open_df.columns:
            open_df[col] = pd.NA
    
    # Asegurarse de que las columnas de fecha tengan el tipo correcto antes de guardar
    if 'purchase_date' in open_df.columns:
        open_df['purchase_date'] = pd.to_datetime(open_df['purchase_date'], errors='coerce')
    if 'expiration_date' in open_df.columns:
        open_df['expiration_date'] = pd.to_datetime(open_df['expiration_date'], errors='coerce')

    open_df[final_open_cols].to_csv(OPEN_POSITIONS_FILE, index=False, date_format='%Y-%m-%d')
    logging.info(f"Reconciliation complete. {len(open_df)} open lots written to {OPEN_POSITIONS_FILE}.")

    # Añadir nuevas operaciones cerradas al archivo existente
    if newly_closed_trades:
        new_closed_df = pd.DataFrame(newly_closed_trades)
        file_exists = os.path.exists(CLOSED_TRADES_FILE) and os.path.getsize(CLOSED_TRADES_FILE) > 0
        
        final_closed_cols = [
            "ticker", "quantity", "buy_date", "buy_price", "buy_currency", 
            "sell_date", "sell_price", "sell_currency", "total_cost_ars", 
            "total_revenue_ars", "total_cost_usd", "total_revenue_usd", 
            "buy_broker_transaction_id", "sell_broker_transaction_id"
        ]
        for col in final_closed_cols:
            if col not in new_closed_df.columns:
                new_closed_df[col] = pd.NA

        new_closed_df[final_closed_cols].to_csv(CLOSED_TRADES_FILE, mode='a', header=not file_exists, index=False, date_format='%Y-%m-%d')
        logging.info(f"{len(new_closed_df)} new trades appended to {CLOSED_TRADES_FILE}.")

if __name__ == "__main__":
    reconcile_portfolio()
