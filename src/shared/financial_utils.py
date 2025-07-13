import pandas as pd
import re


def _get_cpi_value_for_date(
    target_date: pd.Timestamp, cpi_df: pd.DataFrame
) -> float | None:
    """Finds the nearest CPI value for a single date."""
    if cpi_df.empty or pd.isna(target_date):
        return None

    lookup_df = pd.DataFrame({"date": [target_date]})
    merged = pd.merge_asof(
        lookup_df, cpi_df.sort_values("date"), on="date", direction="nearest"
    )

    if merged.empty or pd.isna(merged["value"].iloc[0]):
        return None
    return merged["value"].iloc[0]


def calculate_inflation_period(start_date, end_date, cpi_df: pd.DataFrame) -> float:
    """Calculates cumulative inflation between two dates using a CPI DataFrame."""
    start_val = _get_cpi_value_for_date(pd.to_datetime(start_date), cpi_df)
    end_val = _get_cpi_value_for_date(pd.to_datetime(end_date), cpi_df)

    if start_val is None or end_val is None or start_val == 0:
        return 0.0

    return (end_val / start_val) - 1.0


def map_instrument_to_asset_type(instrument: dict) -> str:
    """Maps a broker's instrument data to a standard asset type."""
    if not instrument:
        return "UNKNOWN"

    instrument_type = instrument.get("type", "").upper()
    op_type = instrument.get("instrumentOperationType", "").upper()

    if op_type == "OPTION":
        return "OPCION"
    if instrument_type == "CEDEAR":
        return "CEDEAR"
    if instrument_type in ["MERVAL", "GENERAL", "LIDER", "PRIVATE_TITLE"]:
        return "ACCION"
    if instrument_type in ["BOND", "LETTER", "PUBLIC_TITLE"]:
        return "RF"
    if op_type == "PUBLIC_TITLE":
        return "RF"
    if op_type == "PRIVATE_TITLE":
        return "ACCION"

    return "UNKNOWN"


def parse_option_details(gallo_name: str) -> dict:
    """Parses option contract details from its name string."""
    if not gallo_name:
        return {}

    cleaned_name = gallo_name.replace(".", "")
    match = re.match(r"([A-Z0-9]+)\s*\((C|V)\)\s*([\d,\.]+)", cleaned_name)

    if not match:
        return {}

    return {
        "underlying_asset": match.group(1),
        "option_type": "CALL" if match.group(2) == "C" else "PUT",
        "strike_price": float(match.group(3).replace(",", ".")),
    }
