import pandas as pd
from portfolio import Portfolio
import data_fetcher
from datetime import datetime


def print_menu():
    """Prints the main menu options."""
    print("\n===== PORTFOLIO TRACKER CLI =====")
    print("1. Record New Transaction")
    print("2. View Open Positions")
    print("3. View Closed Trades History")
    print("4. Update Economic Data (FX and CPI)")
    print("5. Exit")


def parse_number(number_str: str) -> float:
    """Converts a string in local format (e.g., "1.234,56") to a float."""
    cleaned_str = number_str.replace(".", "")
    cleaned_str = cleaned_str.replace(",", ".")
    return float(cleaned_str)


def get_transaction_details():
    """Prompts the user for transaction details, handling local formats."""
    while True:
        op_type = input("Operation type (BUY/SELL): ").upper()
        if op_type in ["BUY", "SELL"]:
            break
        print("Invalid type. Please enter 'BUY' or 'SELL'.")

    while True:
        date = input("Enter date (DD-MM-YYYY): ")
        try:
            date_obj = datetime.strptime(date, "%d-%m-%Y")
            date = date_obj.strftime("%Y-%m-%d")
            break
        except ValueError:
            print("Invalid date format. Please use DD-MM-YYYY.")

    # --- NEW BLOCK FOR ASSET TYPE ---
    while True:
        asset_type = input("Asset type (ACCION, CEDEAR, RF): ").upper()
        if asset_type in ["ACCION", "CEDEAR", "RF"]:
            break
        print("Invalid type. Please enter 'ACCION', 'CEDEAR', or 'RF'.")
    # --------------------------------

    ticker = input("Ticker: ").upper()

    while True:
        try:
            quantity_str = input("Quantity: ")
            quantity = parse_number(quantity_str)
            break
        except ValueError:
            print("Invalid number format. Please use format like 1.234,56")

    while True:
        currency = input("Currency (ARS/USD): ").upper()
        if currency in ["ARS", "USD"]:
            break
        print("Invalid currency. Please enter 'ARS' or 'USD'.")

    while True:
        try:
            price_str = input("Price per unit: ")
            price = parse_number(price_str)
            break
        except ValueError:
            print("Invalid number format. Please use format like 1.234,56")

    while True:
        try:
            market_fees_str = input("Market Fees (Derechos de mercado): ")
            market_fees = parse_number(market_fees_str)
            break
        except ValueError:
            print("Invalid number format. Please use format like 1.234,56")

    while True:
        try:
            taxes_str = input("Taxes (Impuestos): ")
            taxes = parse_number(taxes_str)
            break
        except ValueError:
            print("Invalid number format. Please use format like 1.234,56")

    # Return the new asset_type variable
    return (
        op_type,
        date,
        ticker,
        quantity,
        currency,
        price,
        market_fees,
        taxes,
        asset_type,
    )


def main():
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 1000)

    portfolio = Portfolio()

    while True:
        print_menu()
        choice = input("Select an option: ")

        if choice == "1":
            # Unpack the new asset_type variable
            (
                op_type,
                date,
                ticker,
                quantity,
                currency,
                price,
                market_fees,
                taxes,
                asset_type,
            ) = get_transaction_details()

            # Pass asset_type to the portfolio methods
            if op_type == "BUY":
                portfolio.record_buy(
                    date,
                    ticker,
                    quantity,
                    currency,
                    price,
                    market_fees,
                    taxes,
                    asset_type,
                )
            elif op_type == "SELL":
                portfolio.record_sell(
                    date,
                    ticker,
                    quantity,
                    currency,
                    price,
                    market_fees,
                    taxes,
                    asset_type,
                )

        elif choice == "2":
            portfolio.show_open_positions()

        elif choice == "3":
            portfolio.show_closed_trades()

        elif choice == "4":
            data_fetcher.update_all_exchange_rates()
            data_fetcher.update_cpi_argentina()
            data_fetcher.update_cpi_usa_from_api()
            print("Economic data update process finished.")

        elif choice == "5":
            print("Exiting program.")
            break

        else:
            print("Invalid option. Please try again.")


if __name__ == "__main__":
    main()
