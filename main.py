from typing import Dict
import pandas as pd
from decimal import Decimal
from config import BOND_POSITIONS, CEDEAR_POSITIONS
from yahoo_data import YahooData 
from ppi_data import PPIData 
from models.position import Position 

class PortfolioAnalyzer:
    def __init__(self, ppi_client, yahoo_client):
        self.ppi = ppi_client
        self.yahoo = yahoo_client
        self.ccl = self._get_ccl()
        
    def _get_ccl(self) -> Decimal:
        """Calcula CCL actual usando GGAL"""
        try:
            ggal_cedear = Decimal(str(self.ppi.get_cedear_prices().get('GGALD', 0)))
            ggal_stock = Decimal(str(self.yahoo.get_stock_prices().get('GGAL', 0)))
            if ggal_cedear and ggal_stock:
                return ggal_cedear / (ggal_stock * Decimal('10'))
            return Decimal('1000')
        except Exception as e:
            print(f"Error calculating CCL: {e}")
            return Decimal('1000')

    def analyze_cedears(self, positions: Dict[str, Position]) -> pd.DataFrame:
        cedear_prices = self.ppi.get_cedear_prices()
        stock_prices = self.yahoo.get_stock_prices()
        results = []

        for ticker, position in positions.items():
            current_price_ars = Decimal(str(cedear_prices.get(ticker, 0)))
            underlying = position.ticker.replace('D', '')  # Remove 'D' suffix
            current_price_usd = Decimal(str(stock_prices.get(underlying, 0)))

            if not current_price_ars or not current_price_usd:
                print(f"Warning: Missing prices for {ticker}")
                continue

            # Valores en ARS
            purchase_value_ars = position.purchase_price * position.shares
            current_value_ars = current_price_ars * position.shares
            
            # Valores en USD
            purchase_value_usd = purchase_value_ars / position.purchase_ccl
            current_value_usd = current_value_ars / self.ccl
            
            # Arbitraje
            theoretical_price = (current_price_usd * position.ratio * self.ccl)
            arbitrage = ((current_price_ars / theoretical_price) - Decimal('1')) * Decimal('100')

            results.append({
                'Ticker': ticker,
                'Subyacente': underlying,
                'Fecha Compra': position.purchase_date,
                'CCL Compra': float(position.purchase_ccl),
                'CCL Actual': float(self.ccl),
                'Precio Compra ARS': float(position.purchase_price),
                'Precio Actual ARS': float(current_price_ars),
                'Precio Actual USD': float(current_price_usd),
                'Valor Compra ARS': float(purchase_value_ars),
                'Valor Actual ARS': float(current_value_ars),
                'Valor Compra USD': float(purchase_value_usd),
                'Valor Actual USD': float(current_value_usd),
                'P&L ARS': float(current_value_ars - purchase_value_ars),
                'P&L USD': float(current_value_usd - purchase_value_usd),
                'P&L %': float(((current_value_ars/purchase_value_ars) - Decimal('1')) * Decimal('100')),
                'Arbitraje %': float(arbitrage)
            })

        df = pd.DataFrame(results)
        if not df.empty:
            totals = {
                'Ticker': 'TOTAL',
                'Valor Compra ARS': df['Valor Compra ARS'].sum(),
                'Valor Actual ARS': df['Valor Actual ARS'].sum(),
                'Valor Compra USD': df['Valor Compra USD'].sum(),
                'Valor Actual USD': df['Valor Actual USD'].sum(),
                'P&L ARS': df['P&L ARS'].sum(),
                'P&L USD': df['P&L USD'].sum(),
                'P&L %': ((df['Valor Actual ARS'].sum() / df['Valor Compra ARS'].sum()) - 1) * 100
            }
            df = pd.concat([df, pd.DataFrame([totals])], ignore_index=True)
            
        return df.set_index('Ticker')

    def analyze_bonds(self, positions: Dict[str, Position]) -> pd.DataFrame:
        bond_prices = self.ppi.get_bond_prices()
        results = []

        for ticker, position in positions.items():
            current_price_usd = Decimal(str(bond_prices.get(ticker, 0)))
            
            if not current_price_usd:
                print(f"Warning: Missing price for bond {ticker}")
                continue

            # Valores en USD
            purchase_value_usd = position.purchase_price * position.shares
            current_value_usd = current_price_usd * position.shares
            
            # Valores en ARS
            purchase_value_ars = purchase_value_usd * position.purchase_ccl
            current_value_ars = current_value_usd * self.ccl

            results.append({
                'Ticker': ticker,
                'Fecha Compra': position.purchase_date,
                'CCL Compra': float(position.purchase_ccl),
                'CCL Actual': float(self.ccl),
                'Precio Compra USD': float(position.purchase_price),
                'Precio Actual USD': float(current_price_usd),
                'Valor Compra USD': float(purchase_value_usd),
                'Valor Actual USD': float(current_value_usd),
                'Valor Compra ARS': float(purchase_value_ars),
                'Valor Actual ARS': float(current_value_ars),
                'P&L USD': float(current_value_usd - purchase_value_usd),
                'P&L ARS': float(current_value_ars - purchase_value_ars),
                'P&L %': float(((current_value_usd/purchase_value_usd) - Decimal('1')) * Decimal('100'))
            })

        df = pd.DataFrame(results)
        if not df.empty:
            totals = {
                'Ticker': 'TOTAL',
                'Valor Compra USD': df['Valor Compra USD'].sum(),
                'Valor Actual USD': df['Valor Actual USD'].sum(),
                'Valor Compra ARS': df['Valor Compra ARS'].sum(),
                'Valor Actual ARS': df['Valor Actual ARS'].sum(),
                'P&L USD': df['P&L USD'].sum(),
                'P&L ARS': df['P&L ARS'].sum(),
                'P&L %': ((df['Valor Actual USD'].sum() / df['Valor Compra USD'].sum()) - 1) * 100
            }
            df = pd.concat([df, pd.DataFrame([totals])], ignore_index=True)
            
        return df.set_index('Ticker')

    def analyze(self, cedear_positions: Dict[str, Position], bond_positions: Dict[str, Position]):
        pd.set_option('display.float_format', lambda x: f'{x:,.2f}')
        
        cedears_df = self.analyze_cedears(cedear_positions)
        bonds_df = self.analyze_bonds(bond_positions)
        
        print("\nCEDEARs Analysis:")
        print(cedears_df)
        
        print("\nBonds Analysis:")
        print(bonds_df)
        
        # Portfolio Summary
        total_ars = cedears_df.loc['TOTAL', 'Valor Actual ARS'] + bonds_df.loc['TOTAL', 'Valor Actual ARS']
        total_usd = cedears_df.loc['TOTAL', 'Valor Actual USD'] + bonds_df.loc['TOTAL', 'Valor Actual USD']
        pnl_ars = cedears_df.loc['TOTAL', 'P&L ARS'] + bonds_df.loc['TOTAL', 'P&L ARS']
        pnl_usd = cedears_df.loc['TOTAL', 'P&L USD'] + bonds_df.loc['TOTAL', 'P&L USD']
        
        print("\nPortfolio Summary:")
        print(f"Total Value ARS: {total_ars:,.2f}")
        print(f"Total Value USD: {total_usd:,.2f}")
        print(f"Total P&L ARS: {pnl_ars:,.2f}")
        print(f"Total P&L USD: {pnl_usd:,.2f}")

if __name__ == "__main__":
    pd.set_option('display.float_format', lambda x: '%.2f' % x)
    yahoo_client = YahooData()
    ppi_client = PPIData()
    analyzer = PortfolioAnalyzer(ppi_client=ppi_client, yahoo_client=yahoo_client)
    analyzer.analyze(CEDEAR_POSITIONS, BOND_POSITIONS)