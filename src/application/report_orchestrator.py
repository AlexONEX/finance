import logging
import pandas as pd

from src.infrastructure import data_fetcher
from src.infrastructure.persistence.portfolio_repository import PortfolioRepository
from src.application.reporting_service import ReportingService
from src.application.transaction_service import TransactionService
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

    def _ensure_data_is_updated(self, positions_df):
        """Llama al data_fetcher para actualizar las fuentes de datos necesarias."""
        data_fetcher.update_cer()
        data_fetcher.update_cpi_usa()
        data_fetcher.update_dolar_mep()
        data_fetcher.update_dolar_ccl()

    def generate_and_display_report(self):
        """
        Ejecuta el flujo completo para generar y mostrar el reporte en la consola.
        """
        pd.set_option("display.max_columns", None)
        pd.set_option("display.width", 1000)

        initial_portfolio = self.repository.load_full_portfolio()
        self._ensure_data_is_updated(initial_portfolio.open_positions)
        portfolio = self.repository.load_full_portfolio()
        reporting_service = ReportingService(portfolio)

        transaction_service = TransactionService(portfolio, self.repository)
        transaction_service.expire_options()

        print("\n" + "=" * 50)
        open_positions_report = reporting_service.generate_open_positions_report()
        display_open_positions_report(open_positions_report)

        print("\n" + "=" * 50)
        closed_trades_report = reporting_service.generate_closed_trades_report()
        display_closed_trades_report(closed_trades_report)
        print("\n" + "=" * 50)
