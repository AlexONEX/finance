import logging
import pandas as pd

from src.infrastructure import data_fetcher
from src.infrastructure.persistence.portfolio_repository import PortfolioRepository
from src.application.reporting_service import ReportingService
from src.presentation.cli import (
    display_open_positions_report,
    display_closed_trades_report,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class ReportOrchestrator:
    """
    Orquesta todo el proceso de generación de reportes:
    1. Actualiza los datos de mercado.
    2. Carga el portafolio.
    3. Llama al servicio de reporting para los cálculos.
    4. Llama a las funciones de presentación para mostrar los resultados.
    """

    def __init__(self):
        self.repository = PortfolioRepository()
        # Aquí podrías inicializar otros gateways si fueran necesarios en toda la clase
        # Por ahora, los servicios que lo necesitan los instancian ellos mismos.

    def _ensure_data_is_updated(self, positions_df):
        """Llama al data_fetcher para actualizar las fuentes de datos necesarias."""

        # 1. Actualizar datos macroeconómicos
        data_fetcher.update_cpi_argentina()
        data_fetcher.update_cpi_usa()
        data_fetcher.update_dolar_mep()
        data_fetcher.update_dolar_ccl()

        # 2. Actualizar datos históricos para los activos en cartera
        if not positions_df.empty:
            unique_assets = positions_df[["asset_type", "ticker"]].drop_duplicates()
            for _, row in unique_assets.iterrows():
                # Asegurarse de que el asset_type y ticker son válidos
                if pd.notna(row["asset_type"]) and pd.notna(row["ticker"]):
                    data_fetcher.update_historical_asset(
                        row["asset_type"], row["ticker"]
                    )

    def generate_and_display_report(self):
        """
        Ejecuta el flujo completo para generar y mostrar el reporte en la consola.
        """
        pd.set_option("display.max_columns", None)
        pd.set_option("display.width", 1000)

        # Cargar posiciones para saber qué activos actualizar
        initial_portfolio = self.repository.load_full_portfolio()

        # Paso 1: Actualizar los datos locales (CSVs)
        self._ensure_data_is_updated(initial_portfolio.open_positions)

        # Paso 2: Cargar el portafolio completo con los datos ya actualizados
        portfolio = self.repository.load_full_portfolio()

        # Paso 3: Instanciar el servicio de reporting
        # Pasamos el portfolio completo al servicio que hará los cálculos.
        reporting_service = ReportingService(portfolio)

        # Paso 4: Generar y mostrar los reportes
        print("\n" + "=" * 50)
        open_positions_report = reporting_service.generate_open_positions_report()
        display_open_positions_report(open_positions_report)

        print("\n" + "=" * 50)
        closed_trades_report = reporting_service.generate_closed_trades_report()
        display_closed_trades_report(closed_trades_report)
        print("\n" + "=" * 50)
