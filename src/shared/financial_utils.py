import pandas as pd
import re


def _get_cpi_value_for_date(
    target_date: pd.Timestamp, cpi_df: pd.DataFrame
) -> float | None:
    if cpi_df.empty or pd.isna(target_date):
        return None

    cpi_df["date"] = pd.to_datetime(cpi_df["date"])
    target_date = pd.to_datetime(target_date)

    cpi_df = cpi_df.sort_values("date").reset_index(drop=True)

    last_available_date = cpi_df["date"].max()

    if target_date <= last_available_date:
        lookup_df = pd.DataFrame({"date": [target_date]})
        merged = pd.merge_asof(lookup_df, cpi_df, on="date", direction="nearest")
        return merged["value"].iloc[0] if not merged.empty else None

    else:
        months_diff = (target_date.year - last_available_date.year) * 12 + (
            target_date.month - last_available_date.month
        )
        if len(cpi_df) >= 7:
            recent_cpi = cpi_df.tail(7)
            monthly_returns = recent_cpi["value"].pct_change().dropna()
            avg_monthly_inflation = monthly_returns.mean()
        else:
            avg_monthly_inflation = 0.002
        last_cpi_value = cpi_df["value"].iloc[-1]
        projected_cpi = last_cpi_value * ((1 + avg_monthly_inflation) ** months_diff)
        return projected_cpi


def calculate_inflation_period(start_date, end_date, cpi_df: pd.DataFrame) -> float:
    start_val = _get_cpi_value_for_date(pd.to_datetime(start_date), cpi_df)
    end_val = _get_cpi_value_for_date(pd.to_datetime(end_date), cpi_df)

    if start_val is None or end_val is None or start_val == 0:
        return 0.0

    return (end_val / start_val) - 1.0


def map_instrument_to_asset_type(instrument: dict) -> str:
    if not instrument:
        return "UNKNOWN"

    op_type = instrument.get("instrumentOperationType", "").upper()
    if op_type == "OPTION":
        return "OPTION"

    instrument_type = instrument.get("type", "").upper()
    if instrument_type == "CEDEAR":
        return "CEDEAR"

    known_types = [
        "MERVAL",
        "GENERAL",
        "LIDER",
        "PRIVATE_TITLE",
        "BOND",
        "LETTER",
        "PUBLIC_TITLE",
    ]
    if instrument_type in known_types:
        return instrument_type

    if op_type == "PUBLIC_TITLE":
        return "PUBLIC_TITLE"
    if op_type == "PRIVATE_TITLE":
        return "PRIVATE_TITLE"

    return "UNKNOWN"


def parse_option_details(gallo_name: str) -> dict:
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
