import pandas as pd
from datetime import datetime

from src.infrastructure.persistence.portfolio_repository import PortfolioRepository
from src.application.reporting_service import ReportingService
from src.application.transaction_service import TransactionService


def print_menu():
    print("\n===== PORTFOLIO TRACKER CLI =====")
    print("1. Record New Transaction")
    print("2. View Open Positions (with Performance)")
    print("3. View Closed Trades History")
    print("4. Update Economic Data (FX and CER)")
    print("5. Run Daily Maintenance (Expire Options)")
    print("6. Exit")


def display_open_positions_report(report_data: dict[str, pd.DataFrame]):
    consolidated_df = report_data.get("consolidated", pd.DataFrame())
    print("\n--- Stocks, CEDEARs, Bonds (Consolidated Performance) ---")
    if not consolidated_df.empty:
        display_df = consolidated_df.rename(
            columns={
                "ticker": "Ticker",
                "quantity": "Quantity",
                "buy_price_ars": "Buy Price",
                "current_price": "Current Price",
                "nominal_return_ars_pct": "Return ARS (%)",
                "real_return_ars_pct": "Real Return ARS (%)",
                "age_days": "Avg. Days",
            }
        )

        for col in ["Return ARS (%)", "Real Return ARS (%)"]:
            if col in display_df:
                display_df[col] = display_df[col].apply(
                    lambda x: f"{x:+.2f}%" if pd.notna(x) else "N/A"
                )

        for col in ["Buy Price", "Current Price"]:
            if col in display_df:
                display_df[col] = display_df[col].apply(
                    lambda x: f"{x:,.2f}" if pd.notna(x) else "N/A"
                )

        cols = [
            "Ticker",
            "Quantity",
            "Buy Price",
            "Current Price",
            "Return ARS (%)",
            "Real Return ARS (%)",
            "Avg. Days",
        ]
        print(display_df[cols].to_string(index=False))
    else:
        print("No open positions in Stocks, CEDEARs, or Bonds.")

    options_df = report_data.get("options", pd.DataFrame())
    print("\n--- Options (Holdings) ---")
    if not options_df.empty:
        options_df["purchase_date"] = pd.to_datetime(
            options_df["purchase_date"]
        ).dt.strftime("%d-%m-%Y")
        option_cols = ["purchase_date", "ticker", "quantity", "total_cost_ars"]
        print(options_df[option_cols].round(2).to_string(index=False))
    else:
        print("No open options positions.")


def display_closed_trades_report(report_df: pd.DataFrame):
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
    return float(number_str.replace(".", "").replace(",", "."))


def get_validated_input(prompt: str, validation_func, error_msg: str):
    while True:
        try:
            value = input(prompt)
            return validation_func(value)
        except (ValueError, TypeError):
            print(error_msg)


def get_transaction_details() -> dict:
    op_type = get_validated_input(
        "Operation type (BUY/SELL): ",
        lambda v: v.upper() if v.upper() in ["BUY", "SELL"] else ValueError("err"),
        "Invalid type. Please enter 'BUY' or 'SELL'.",
    )
    date_obj = get_validated_input(
        "Enter date (DD-MM-YYYY) [default: today]: ",
        lambda v: datetime.today() if not v else datetime.strptime(v, "%d-%m-%Y"),
        "Invalid date format. Please use DD-MM-YYYY.",
    )
    asset_type = get_validated_input(
        "Asset type (ACCION, CEDEAR, RF, OPCION): ",
        lambda v: v.upper()
        if v.upper() in ["ACCION", "CEDEAR", "RF", "OPCION"]
        else int("err"),
        "Invalid type. Use ACCION, CEDEAR, RF, or OPCION.",
    )
    ticker = input("Ticker: ").upper()
    prompt_quantity = "Quantity (lots for options, units for others): "
    quantity = get_validated_input(
        prompt_quantity, parse_local_number, "Invalid number format."
    )
    currency = get_validated_input(
        "Currency (ARS/USD): ",
        lambda v: v.upper() if v.upper() in ["ARS", "USD"] else int("err"),
        "Invalid currency.",
    )
    prompt_price = "Price per unit (or per 100 V/N for bonds, or premium for options): "
    price = get_validated_input(
        prompt_price, parse_local_number, "Invalid number format."
    )
    market_fees = get_validated_input(
        "Market Fees: ", parse_local_number, "Invalid number format."
    )
    broker_fees = get_validated_input(
        "Broker Fees: ", parse_local_number, "Invalid number format."
    )
    taxes = get_validated_input("Taxes: ", parse_local_number, "Invalid number format.")

    details = {
        "op_type": op_type,
        "date": date_obj,
        "ticker": ticker,
        "asset_type": asset_type,
        "quantity": quantity,
        "currency": currency,
        "price": price,
        "market_fees": market_fees,
        "broker_fees": broker_fees,
        "taxes": taxes,
    }

    if asset_type == "OPCION":
        exp_date_obj = get_validated_input(
            "Enter expiration date (DD-MM-YYYY): ",
            lambda v: datetime.strptime(v, "%d-%m-%Y"),
            "Invalid date format. Please use DD-MM-YYYY.",
        )
        details["expiration_date"] = exp_date_obj

    return details


def main():
    """Main function to run the CLI application."""
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 1000)

    print("Initializing services...")
    repository = PortfolioRepository()
    print("Services initialized successfully.")

    while True:
        portfolio = repository.load_full_portfolio()
        reporting_service = ReportingService(portfolio)
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
            print("\nFetching market data and calculating performance...")
            from src.infrastructure import data_fetcher

            data_fetcher.update_cer()
            data_fetcher.update_cpi_usa()
            data_fetcher.update_dolar_mep()
            data_fetcher.update_dolar_ccl()

            updated_portfolio = repository.load_full_portfolio()
            updated_reporting_service = ReportingService(updated_portfolio)

            report_data = updated_reporting_service.generate_open_positions_report()
            display_open_positions_report(report_data)

        elif choice == "3":
            report_df = reporting_service.generate_closed_trades_report()
            display_closed_trades_report(report_df)

        elif choice == "4":
            print("\nStarting economic data update...")
            from src.infrastructure import data_fetcher

            data_fetcher.update_cer()
            data_fetcher.update_cpi_usa()
            data_fetcher.update_dolar_mep()
            data_fetcher.update_dolar_ccl()
            print("Economic data update process finished.")

        elif choice == "5":
            print("\nRunning daily maintenance tasks...")
            transaction_service.expire_options()
            print("Maintenance tasks finished.")

        elif choice == "6":
            print("Exiting program.")
            break
        else:
            print("Invalid option. Please try again.")
