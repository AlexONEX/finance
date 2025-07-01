import pandas as pd
from portfolio import Portfolio
import data_fetcher


def print_menu():
    """Prints the main menu options."""
    print("\n===== PORTFOLIO TRACKER CLI =====")
    print("1. Record New Transaction")
    print("2. View Open Positions")
    print("3. View Closed Trades History")
    print("4. Update Economic Data (FX and CPI)")
    print("5. Exit")


def get_transaction_details():
    """Prompts the user for transaction details."""
    while True:
        op_type = input("Operation type (BUY/SELL): ").upper()
        if op_type in ["BUY", "SELL"]:
            break
        print("Invalid type. Please enter 'BUY' or 'SELL'.")

    date = input("Date (YYYY-MM-DD): ")
    ticker = input("Ticker: ").upper()

    while True:
        try:
            quantity = float(input("Quantity: "))
            break
        except ValueError:
            print("Invalid quantity. Please enter a number.")

    while True:
        currency = input("Currency (ARS/USD): ").upper()
        if currency in ["ARS", "USD"]:
            break
        print("Invalid currency. Please enter 'ARS' or 'USD'.")

    while True:
        try:
            price = float(input("Price per unit: "))
            break
        except ValueError:
            print("Invalid price. Please enter a number.")

    return op_type, date, ticker, quantity, currency, price


def main():
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 1000)

    portfolio = Portfolio()

    while True:
        print_menu()
        choice = input("Select an option: ")

        if choice == "1":
            op_type, date, ticker, quantity, currency, price = get_transaction_details()
            if op_type == "BUY":
                portfolio.record_buy(date, ticker, quantity, currency, price)
            elif op_type == "SELL":
                portfolio.record_sell(date, ticker, quantity, currency, price)

        elif choice == "2":
            portfolio.show_open_positions()

        elif choice == "3":
            portfolio.show_closed_trades()

        elif choice == "4":
            data_fetcher.update_exchange_rates()
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
