# portfolio.py
import pandas as pd
import time
import os # Importamos os para chequear el tamaño del archivo
# La referencia a MarketDataService se puede eliminar si no se usa en el futuro
# from services.market_data_service import MarketDataService

class Portfolio:
    positions_file = "data/open_positions.csv"
    trades_file = "data/closed_trades.csv"
    dolar_mep_file = "data/dolar_mep.csv"
    dolar_ccl_file = "data/dolar_ccl.csv"
    cpi_arg_file = "data/cpi_argentina.csv"
    cpi_usa_file = "data/cpi_usa.csv"

    def __init__(self):
        self.load_data()
        # self.market_data_service = MarketDataService()

    def load_data(self):
        """
        Carga los archivos CSV, manejando de forma robusta la posibilidad de que no existan o estén vacíos.
        """
        # --- Lógica robusta para open_positions.csv ---
        open_positions_cols = ["purchase_id", "purchase_date", "ticker", "quantity", "total_cost_ars", "total_cost_usd", "asset_type", "original_price", "original_currency", "underlying_asset", "option_type", "strike_price", "expiration_date", "broker_transaction_id"]
        try:
            if os.path.exists(self.positions_file) and os.path.getsize(self.positions_file) > 0:
                self.open_positions = pd.read_csv(self.positions_file, parse_dates=["purchase_date", "expiration_date"])
            else:
                raise FileNotFoundError
        except (FileNotFoundError, pd.errors.EmptyDataError):
            self.open_positions = pd.DataFrame(columns=open_positions_cols)

        # --- Lógica robusta para closed_trades.csv ---
        closed_trades_cols = ["ticker", "quantity", "buy_date", "sell_date", "total_cost_ars", "total_revenue_ars", "total_cost_usd", "total_revenue_usd", "buy_broker_transaction_id", "sell_broker_transaction_id"]
        try:
            if os.path.exists(self.trades_file) and os.path.getsize(self.trades_file) > 0:
                self.closed_trades = pd.read_csv(self.trades_file, parse_dates=["buy_date", "sell_date"])
            else:
                raise FileNotFoundError
        except (FileNotFoundError, pd.errors.EmptyDataError):
            self.closed_trades = pd.DataFrame(columns=closed_trades_cols)
        
        # Carga de archivos de datos económicos
        try:
            self.dolar_mep = pd.read_csv(self.dolar_mep_file, parse_dates=["date"])
        except FileNotFoundError: self.dolar_mep = pd.DataFrame(columns=["date", "value"])
        try:
            self.dolar_ccl = pd.read_csv(self.dolar_ccl_file, parse_dates=["date"])
        except FileNotFoundError: self.dolar_ccl = pd.DataFrame(columns=["date", "value"])
        try:
            self.cpi_arg = pd.read_csv(self.cpi_arg_file, parse_dates=["date"])
        except FileNotFoundError: self.cpi_arg = pd.DataFrame(columns=["date", "value"])
        try:
            self.cpi_usa = pd.read_csv(self.cpi_usa_file, parse_dates=["date"])
        except FileNotFoundError: self.cpi_usa = pd.DataFrame(columns=["date", "value"])

    # El resto de los métodos permanecen igual, ya que la lógica de cálculo
    # y visualización que construimos es correcta.
    
    def get_inflation(self, start_date, end_date, cpi_df):
        if cpi_df.empty: return 0.0
        cpi_df.sort_values("date", inplace=True)
        start_cpi_row = pd.merge_asof(pd.DataFrame({"date": [pd.to_datetime(start_date)]}), cpi_df, on="date", direction="nearest")
        end_cpi_row = pd.merge_asof(pd.DataFrame({"date": [pd.to_datetime(end_date)]}), cpi_df, on="date", direction="nearest")
        if (start_cpi_row.empty or end_cpi_row.empty or pd.isna(start_cpi_row["value"].iloc[0]) or pd.isna(end_cpi_row["value"].iloc[0])): return 0.0
        start_val, end_val = start_cpi_row["value"].iloc[0], end_cpi_row["value"].iloc[0]
        return (end_val / start_val) -1 if start_val > 0 else 0.0

    def show_open_positions(self):
        if self.open_positions.empty:
            print("No open positions.")
            return
        print("\n--- OPEN POSITIONS (BY PURCHASE LOT) ---")
        display_df = self.open_positions.copy()
        
        display_df["purchase_date"] = pd.to_datetime(display_df["purchase_date"]).dt.strftime("%d-%m-%Y")
        if 'expiration_date' in display_df.columns: display_df['expiration_date'] = pd.to_datetime(display_df['expiration_date']).dt.strftime('%d-%m-%Y')
        
        base_cols, option_cols = ["purchase_date", "ticker", "quantity", "total_cost_ars", "total_cost_usd"], ["underlying_asset", "option_type", "strike_price", "expiration_date"]
        
        for col in base_cols + option_cols:
            if col not in display_df.columns: display_df[col] = pd.NA
        
        options_df = display_df[display_df['asset_type'] == 'OPCION'] if 'asset_type' in display_df.columns else pd.DataFrame()
        other_df = display_df[~display_df['asset_type'].isin(['OPCION'])] if 'asset_type' in display_df.columns else display_df

        print("\n--- Stocks, CEDEARs, Bonds ---")
        if not other_df.empty: print(other_df[base_cols].round(2).to_string(index=False))
        else: print("No positions.")

        print("\n--- Options ---")
        if not options_df.empty: print(options_df[base_cols + option_cols].fillna('').round(2).to_string(index=False))
        else: print("No options positions.")

    def show_closed_trades(self):
        if self.closed_trades.empty:
            print("No closed trades recorded.")
            return
            
        df = self.closed_trades.copy()
        df['nominal_return_ars_pct'] = ((df['total_revenue_ars'] / df['total_cost_ars']) - 1) * 100
        df['nominal_return_usd_pct'] = ((df['total_revenue_usd'] / df['total_cost_usd']) - 1) * 100
        
        df['inflation_arg'] = df.apply(lambda row: self.get_inflation(row['buy_date'], row['sell_date'], self.cpi_arg), axis=1)
        df['inflation_usa'] = df.apply(lambda row: self.get_inflation(row['buy_date'], row['sell_date'], self.cpi_usa), axis=1)
        df['real_return_ars_pct'] = (((1 + df['nominal_return_ars_pct'] / 100) / (1 + df['inflation_arg'])) - 1) * 100
        df['real_return_usd_pct'] = (((1 + df['nominal_return_usd_pct'] / 100) / (1 + df['inflation_usa'])) - 1) * 100

        print("\n--- CLOSED TRADES HISTORY ---")
        display_cols = {"ticker": "Ticker", "quantity": "Quantity", "buy_date": "Buy Date", "sell_date": "Sell Date", "nominal_return_ars_pct": "Nom. Ret. ARS (%)", "real_return_ars_pct": "Real Ret. ARS (%)", "nominal_return_usd_pct": "Nom. Ret. USD (%)", "real_return_usd_pct": "Real Ret. USD (%)"}
        
        df['buy_date'] = pd.to_datetime(df['buy_date']).dt.strftime('%d-%m-%Y')
        df['sell_date'] = pd.to_datetime(df['sell_date']).dt.strftime('%d-%m-%Y')
        
        display_df = df[[col for col in display_cols if col in df.columns]].rename(columns=display_cols)
        print(display_df.round(2).to_string(index=False))
