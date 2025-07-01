import pandas as pd
from gateway.alpha_vantage_connector import AlphaVantageAPIConnector
import logging


class MarketDataService:
    def __init__(self):
        self.connector = AlphaVantageAPIConnector()

    def get_market_data(self, positions_df: pd.DataFrame) -> pd.DataFrame:
        if positions_df.empty or not all(
            col in positions_df.columns for col in ["ticker", "asset_type"]
        ):
            logging.warning(
                "Positions DataFrame is empty or missing required columns ('ticker', 'asset_type')."
            )
            return pd.DataFrame(
                columns=["ticker", "market_price_native", "daily_change_pct"]
            )

        # Get unique combinations of ticker and asset type to avoid redundant API calls
        unique_positions = positions_df.drop_duplicates(subset=["ticker", "asset_type"])
        market_data_list = []

        for _, row in unique_positions.iterrows():
            ticker = row["ticker"]
            asset_type = row.get("asset_type", "N/A")

            api_ticker = ticker
            if asset_type == "ACCION":
                api_ticker = f"{ticker}.BA"
            elif asset_type == "RF":
                logging.warning(
                    f"Market data for Fixed Income ticker '{ticker}' is not supported."
                )
                continue

            quote = self.connector.get_quote_endpoint(api_ticker)

            if quote:
                market_data_list.append(
                    {
                        "ticker": ticker,  # Use original ticker for merging
                        "market_price_native": quote.get("price"),
                        "daily_change_pct": quote.get("change_percent"),
                    }
                )
            else:
                logging.warning(
                    f"Could not fetch market data for ticker '{api_ticker}'."
                )

        if not market_data_list:
            return pd.DataFrame(
                columns=["ticker", "market_price_native", "daily_change_pct"]
            )

        return pd.DataFrame(market_data_list)
