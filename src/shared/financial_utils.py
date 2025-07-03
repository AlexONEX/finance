import pandas as pd


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
