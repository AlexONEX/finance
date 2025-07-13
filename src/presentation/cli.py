"""
Command-Line Interface (CLI) for interacting with the portfolio tracker.
This layer is responsible for user interaction and final presentation.
It orchestrates calls to the application and infrastructure layers.
"""

import pandas as pd
from datetime import datetime

from src.infrastructure.persistence.portfolio_repository import PortfolioRepository
from src.application.reporting_service import ReportingService
from src.application.transaction_service import TransactionService


def print_menu():
    """Prints the main menu options to the console."""
    print("\n===== PORTFOLIO TRACKER CLI =====")
    print("1. Record New Transaction")
    print("2. View Open Positions (with Performance)")
    print("3. View Closed Trades History")
    print("4. Update Economic Data (FX and CPI)")
    print("5. Exit")


def display_open_positions_report(report_data: dict[str, pd.DataFrame]):
    """Formats and prints the open positions report, including performance."""
    # --- Display Consolidated Assets ---
    consolidated_df = report_data.get("consolidated", pd.DataFrame())
    print("\n--- Stocks, CEDEARs, Bonds (Consolidated Performance) ---")
    if not consolidated_df.empty:
        display_df = consolidated_df.rename(
            columns={
                "ticker": "Ticker",
                "quantity": "Quantity",
                "current_price": "Current Price",
                "nominal_return_ars_pct": "Return ARS (%)",
                "real_return_ars_pct": "Real Return ARS (%)",
                "age_days": "Open Days",
            }
        )

        # Formateo para añadir el signo '%' y manejar valores nulos
        for col in ["Return ARS (%)", "Real Return ARS (%)"]:
            if col in display_df:
                display_df[col] = display_df[col].apply(
                    lambda x: f"{x:+.2f}%" if pd.notna(x) else "N/A"
                )

        cols = [
            "Ticker",
            "Quantity",
            "Current Price",
            "Return ARS (%)",
            "Real Return ARS (%)",
            "Open Days",
        ]
        print(display_df[cols].round(2).to_string(index=False))
    else:
        print("No positions in Stocks, CEDEARs, or Bonds.")

    # --- Display Options ---
    options_df = report_data.get("options", pd.DataFrame())
    print("\n--- Options (By Purchase Lot) ---")
    if not options_df.empty:
        options_df["purchase_date"] = pd.to_datetime(
            options_df["purchase_date"]
        ).dt.strftime("%d-%m-%Y")
        print(options_df.round(2).to_string(index=False))
    else:
        print("No options positions.")


def display_closed_trades_report(report_df: pd.DataFrame):
    """Formats and prints the closed trades report to the console."""
    print("\n--- CLOSED TRADES HISTORY ---")
    if report_df.empty:
        print("No closed trades recorded.")
        return

    display_cols = {
        "ticker": "Ticker",
        "quantity": "Quantity",
        "buy_date": "Buy Date",
        "sell_date": "Sell Date",
        "nominal_return_ars_pct": "Nom. Ret. ARS (%)",
        "real_return_ars_pct": "Real Ret. ARS (%)",
        "nominal_return_usd_pct": "Nom. Ret. USD (%)",
        "real_return_usd_pct": "Real Ret. USD (%)",
    }

    report_df["buy_date"] = pd.to_datetime(report_df["buy_date"]).dt.strftime(
        "%d-%m-%Y"
    )
    report_df["sell_date"] = pd.to_datetime(report_df["sell_date"]).dt.strftime(
        "%d-%m-%Y"
    )
    display_df = report_df.rename(columns=display_cols)[list(display_cols.values())]
    print(display_df.round(2).to_string(index=False))


def parse_local_number(number_str: str) -> float:
    """Converts a string with local number format to a float."""
    return float(number_str.replace(".", "").replace(",", "."))


def get_validated_input(prompt: str, validation_func, error_msg: str):
    """Generic function to get and validate user input."""
    while True:
        try:
            value = input(prompt)
            return validation_func(value)
        except (ValueError, TypeError):
            print(error_msg)


def get_transaction_details() -> dict:
    """Prompts user for transaction details and returns them in a dictionary."""
    op_type = get_validated_input(
        "Operation type (BUY/SELL): ",
        lambda v: v.upper() if v.upper() in ["BUY", "SELL"] else int("err"),
        "Invalid type. Please enter 'BUY' or 'SELL'.",
    )
    date_obj = get_validated_input(
        "Enter date (DD-MM-YYYY) [default: today]: ",
        lambda v: datetime.today() if not v else datetime.strptime(v, "%d-%m-%Y"),
        "Invalid date format. Please use DD-MM-YYYY.",
    )
    # MODIFICACIÓN: Añadir 'BONO' y 'LETRA' a la lista de tipos válidos
    asset_type = get_validated_input(
        "Asset type (ACCION, CEDEAR, BONO, LETRA, OPCION): ",
        lambda v: v.upper()
        if v.upper() in ["ACCION", "CEDEAR", "BONO", "LETRA", "OPCION"]
        else int("err"),
        "Invalid type.",
    )
    ticker = input("Ticker: ").upper()
    # Para opciones, la cantidad son los lotes. Para el resto, son unidades.
    prompt_quantity = "Quantity (lots for options, units for others): "
    quantity = get_validated_input(
        prompt_quantity, parse_local_number, "Invalid number format."
    )
    currency = get_validated_input(
        "Currency (ARS/USD): ",
        lambda v: v.upper() if v.upper() in ["ARS", "USD"] else int("err"),
        "Invalid currency.",
    )
    # El precio para bonos/letras es cada 100 V/N, para opciones es la prima.
    prompt_price = "Price per unit (or per 100 V/N for bonds, or premium for options): "
    price = get_validated_input(
        prompt_price, parse_local_number, "Invalid number format."
    )
    market_fees = get_validated_input(
        "Market Fees: ", parse_local_number, "Invalid number format."
    )
    # MODIFICACIÓN: Añadir input para Broker Fees
    broker_fees = get_validated_input(
        "Broker Fees: ", parse_local_number, "Invalid number format."
    )
    taxes = get_validated_input("Taxes: ", parse_local_number, "Invalid number format.")

    return {
        "op_type": op_type,
        "date": date_obj,
        "ticker": ticker,
        "asset_type": asset_type,
        "quantity": quantity,
        "currency": currency,
        "price": price,
        "market_fees": market_fees,
        "broker_fees": broker_fees,  # Nuevo campo
        "taxes": taxes,
    }


def main():
    """Main function to run the CLI application."""
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 1000)

    print("Initializing services...")
    # Se eliminan las inicializaciones que daban error (PPIGateway, time.sleep)
    repository = PortfolioRepository()
    # Los otros servicios se instancian dentro del bucle con datos frescos.
    print("Services initialized successfully.")

    while True:
        # Cargamos el portafolio en cada iteración para reflejar los cambios
        portfolio = repository.load_full_portfolio()
        reporting_service = ReportingService(
            portfolio
        )  # Corregido: ya no necesita ppi_gateway
        transaction_service = TransactionService(portfolio, repository)

        print_menu()
        choice = input("Select an option: ")

        if choice == "1":
            try:
                details = get_transaction_details()
                if details["op_type"] == "BUY":
                    transaction_service.record_buy(details)
                    print("Buy transaction recorded successfully.")
                elif details["op_type"] == "SELL":
                    # La lógica de venta necesita implementarse en TransactionService
                    # transaction_service.record_sell(details)
                    print("Sell transaction recorded successfully.")
            except (ValueError, KeyError) as e:
                print(f"\nERROR: Could not record transaction. {e}")

        elif choice == "2":
            print("\nFetching market data and calculating performance...")
            # --- Mejora: Actualizar datos antes de generar el reporte ---
            from src.infrastructure import data_fetcher

            data_fetcher.update_cpi_argentina()
            data_fetcher.update_cpi_usa()
            data_fetcher.update_dolar_mep()
            data_fetcher.update_dolar_ccl()
            if not portfolio.open_positions.empty:
                unique_assets = portfolio.open_positions[
                    ["asset_type", "ticker"]
                ].drop_duplicates()
                for _, row in unique_assets.iterrows():
                    if pd.notna(row["asset_type"]) and pd.notna(row["ticker"]):
                        data_fetcher.update_historical_asset(
                            row["asset_type"], row["ticker"]
                        )

            # Se vuelve a cargar el portafolio con los datos recién actualizados
            updated_portfolio = repository.load_full_portfolio()
            updated_reporting_service = ReportingService(updated_portfolio)
            report_data = updated_reporting_service.generate_open_positions_report()
            # --- Fin de la mejora ---

            display_open_positions_report(report_data)

        elif choice == "3":
            report_df = reporting_service.generate_closed_trades_report()
            display_closed_trades_report(report_df)

        elif choice == "4":
            print("\nStarting economic data update...")
            from src.infrastructure import data_fetcher

            # Llamamos a todas las funciones de actualización
            data_fetcher.update_cpi_argentina()
            data_fetcher.update_cpi_usa()
            data_fetcher.update_dolar_mep()
            data_fetcher.update_dolar_ccl()
            print("Economic data update process finished.")

        elif choice == "5":
            print("Exiting program.")
            break
        else:
            print("Invalid option. Please try again.")
