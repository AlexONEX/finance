import pandas as pd
from datetime import datetime
from src.domain.portfolio import Portfolio
from src.infrastructure import data_fetcher


def print_menu():
    """Prints the main menu options."""
    print("\n===== PORTFOLIO TRACKER CLI =====")
    print("1. Record New Transaction")
    print("2. View Open Positions")
    print("3. View Closed Trades History")
    print("4. Update Economic Data")
    print("5. Exit")


def parse_local_number(number_str: str) -> float:
    """Converts a string with local number format (e.g., "1.234,56") to a float."""
    return float(number_str.replace(".", "").replace(",", "."))


def get_validated_input(prompt: str, validation_func, error_msg: str):
    """Generic function to get and validate user input."""
    while True:
        try:
            value = input(prompt)
            return validation_func(value)
        except (ValueError, TypeError):
            print(error_msg)


def get_transaction_details() -> tuple:
    """Prompts the user for all necessary transaction details."""
    op_type = get_validated_input(
        "Operation type (BUY/SELL): ",
        lambda v: v.upper() if v.upper() in ["BUY", "SELL"] else int("err"),
        "Invalid type. Please enter 'BUY' or 'SELL'.",
    )
    date_str = get_validated_input(
        "Enter date (DD-MM-YYYY): ",
        lambda v: datetime.strptime(v, "%d-%m-%Y").strftime("%Y-%m-%d"),
        "Invalid date format. Please use DD-MM-YYYY.",
    )
    asset_type = get_validated_input(
        "Asset type (ACCION, CEDEAR, RF): ",
        lambda v: v.upper() if v.upper() in ["ACCION", "CEDEAR", "RF"] else int("err"),
        "Invalid type. Please enter 'ACCION', 'CEDEAR', or 'RF'.",
    )
    ticker = input("Ticker: ").upper()
    quantity = get_validated_input(
        "Quantity: ", parse_local_number, "Invalid number format."
    )
    currency = get_validated_input(
        "Currency (ARS/USD): ",
        lambda v: v.upper() if v.upper() in ["ARS", "USD"] else int("err"),
        "Invalid currency. Please enter 'ARS' or 'USD'.",
    )
    price = get_validated_input(
        "Price per unit: ", parse_local_number, "Invalid number format."
    )
    market_fees = get_validated_input(
        "Market Fees: ", parse_local_number, "Invalid number format."
    )
    taxes = get_validated_input("Taxes: ", parse_local_number, "Invalid number format.")

    return (
        op_type,
        date_str,
        ticker,
        quantity,
        currency,
        price,
        market_fees,
        taxes,
        asset_type,
    )


def main():
    """Main function to run the CLI application."""
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 1000)
    portfolio = Portfolio()

    while True:
        print_menu()
        choice = input("Select an option: ")

        if choice == "1":
            details = get_transaction_details()
            op_type, date, ticker, qty, curr, price, fees, taxes, asset = details
            if op_type == "BUY":
                portfolio.record_buy(date, ticker, qty, curr, price, fees, taxes, asset)
            elif op_type == "SELL":
                portfolio.record_sell(
                    date, ticker, qty, curr, price, fees, taxes, asset
                )
        elif choice == "2":
            portfolio.show_open_positions()
        elif choice == "3":
            portfolio.show_closed_trades()
        elif choice == "4":
            data_fetcher.update_all_data()
        elif choice == "5":
            print("Exiting program.")
            break
        else:
            print("Invalid option. Please try again.")


if __name__ == "__main__":
    main()
