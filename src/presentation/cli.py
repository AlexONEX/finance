"""
Command-Line Interface (CLI) for interacting with the portfolio tracker.
This layer is responsible for user interaction and final presentation.
It orchestrates calls to the application and infrastructure layers.
"""

import pandas as pd
from datetime import datetime
import time

# Componentes de otras capas de la arquitectura
from src.infrastructure.persistence.portfolio_repository import PortfolioRepository
from src.application.reporting_service import ReportingService
from src.application.transaction_service import TransactionService
from src.infrastructure.gateways import PPIGateway


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
        "Enter date (DD-MM-YYYY): ",
        lambda v: datetime.strptime(v, "%d-%m-%Y"),
        "Invalid date format. Please use DD-MM-YYYY.",
    )
    asset_type = get_validated_input(
        "Asset type (ACCION, CEDEAR, RF): ",
        lambda v: v.upper() if v.upper() in ["ACCION", "CEDEAR", "RF"] else int("err"),
        "Invalid type.",
    )
    ticker = input("Ticker: ").upper()
    quantity = get_validated_input(
        "Quantity: ", parse_local_number, "Invalid number format."
    )
    currency = get_validated_input(
        "Currency (ARS/USD): ",
        lambda v: v.upper() if v.upper() in ["ARS", "USD"] else int("err"),
        "Invalid currency.",
    )
    price = get_validated_input(
        "Price per unit: ", parse_local_number, "Invalid number format."
    )
    market_fees = get_validated_input(
        "Market Fees: ", parse_local_number, "Invalid number format."
    )
    taxes = get_validated_input("Taxes: ", parse_local_number, "Invalid number format.")

    return {
        "op_type": op_type,
        "date": date_obj,
        "ticker": ticker,
        "quantity": quantity,
        "currency": currency,
        "price": price,
        "market_fees": market_fees,
        "taxes": taxes,
        "asset_type": asset_type,
    }


def main():
    """Main function to run the CLI application."""
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 1000)

    print("Initializing services... Please wait.")
    try:
        repository = PortfolioRepository()
        ppi_gateway = PPIGateway()
        print("Giving services time to connect...")
        time.sleep(5)  # Dar tiempo a la conexión de RT para que se establezca
        print("Services initialized successfully.")
    except Exception as e:
        print(f"FATAL: Could not initialize services. Error: {e}")
        return

    while True:
        # Cargar el estado más reciente del portafolio en cada iteración del menú
        # para reflejar los cambios de las transacciones.
        portfolio = repository.load_full_portfolio()
        reporting_service = ReportingService(portfolio, ppi_gateway)
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
                    transaction_service.record_sell(details)
                    print("Sell transaction recorded successfully.")
            except (ValueError, KeyError) as e:
                print(f"\nERROR: Could not record transaction. {e}")

        elif choice == "2":
            print("\nFetching real-time data and calculating performance...")
            report_data = reporting_service.generate_open_positions_report()
            display_open_positions_report(report_data)

        elif choice == "3":
            report_df = reporting_service.generate_closed_trades_report()
            display_closed_trades_report(report_df)

        elif choice == "4":
            print("\nStarting economic data update...")
            # data_fetcher.update_all_data() # Esta función debería ser llamada aquí
            print("Economic data update process finished.")

        elif choice == "5":
            print("Exiting program.")
            break
        else:
            print("Invalid option. Please try again.")


if __name__ == "__main__":
    main()
