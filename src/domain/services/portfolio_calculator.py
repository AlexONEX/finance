import pandas as pd
import logging

# Local application imports
import config
from domain.portfolio import Portfolio
from application.reporting_service import ReportingService
from infrastructure import data_fetcher
from infrastructure.gateways import PPIGateway  # For real-time prices


class ReportManager:
    """
    Orchestrates the entire report generation process.
    """

    def __init__(self):
        """
        Initializes the manager by loading primary data sources.
        """
        try:
            self.positions = pd.read_csv(config.OPEN_POSITIONS_FILE)
            self.trades = pd.read_csv(config.CLOSED_TRADES_FILE)
        except FileNotFoundError as e:
            logging.error(f"Initial data file not found: {e}. Please create it first.")
            self.positions = pd.DataFrame(columns=["ticker", "asset_type"])
            self.trades = pd.DataFrame()

    def _ensure_data_is_updated(self):
        """
        Calls the data_fetcher to update all required data sources on-demand.
        """
        logging.info("--- Starting on-demand data update ---")

        # 1. Update macroeconomic data
        data_fetcher.update_cpi_argentina()
        data_fetcher.update_cpi_usa()
        data_fetcher.update_dolar_mep()
        data_fetcher.update_dolar_ccl()

        # 2. Update historical data for assets in the portfolio
        if not self.positions.empty:
            # Create a set of unique assets to avoid duplicate calls
            unique_assets = self.positions[["asset_type", "ticker"]].drop_duplicates()
            for _, row in unique_assets.iterrows():
                data_fetcher.update_historical_asset(row["asset_type"], row["ticker"])

        logging.info("--- On-demand data update finished ---")

    def _load_full_portfolio(self) -> Portfolio:
        """
        Loads all updated data from CSV files into a Portfolio domain object.
        """
        logging.info("Loading all updated data into portfolio object...")
        return Portfolio(
            open_positions=self.positions,
            closed_trades=self.trades,
            cpi_arg=pd.read_csv(config.CPI_ARG_FILE),
            cpi_usa=pd.read_csv(config.CPI_USA_FILE),
            dolar_mep=pd.read_csv(config.DOLAR_MEP_FILE),
            dolar_ccl=pd.read_csv(config.DOLAR_CCL_FILE),
            # Add other necessary dataframes here
        )

    def generate_full_report(self):
        """
        Executes the full process: update, load, and generate report.
        """
        # Step 1: Update local data files
        self._ensure_data_is_updated()

        # Step 2: Load updated data into the domain model
        portfolio = self._load_full_portfolio()

        # Step 3: Instantiate gateways and services
        ppi_gateway = PPIGateway()  # Gateway for real-time prices
        reporting_service = ReportingService(portfolio, ppi_gateway)

        # Step 4: Generate and display reports
        logging.info("--- Generating Open Positions Report ---")
        open_positions_report = reporting_service.generate_open_positions_report()
        print(open_positions_report.get("consolidated"))

        logging.info("--- Generating Closed Trades Report ---")
        closed_trades_report = reporting_service.generate_closed_trades_report()
        print(closed_trades_report)

        return {
            "open_positions": open_positions_report,
            "closed_trades": closed_trades_report,
        }


if __name__ == "__main__":
    # This is how you would run the entire process
    manager = ReportManager()
    final_reports = manager.generate_full_report()
