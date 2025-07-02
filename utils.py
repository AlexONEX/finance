import re


def map_instrument_to_asset_type(instrument: dict) -> str:
    """
    Maps an instrument dictionary from the broker to a standardized asset type.

    Args:
        instrument: The instrument dictionary from the transaction data.

    Returns:
        A string representing the standardized asset type (e.g., "ACCION", "RF", "OPCION").
    """
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

    # Fallback for unknown types
    return "UNKNOWN"


def parse_option_details(gallo_name: str) -> dict:
    """
    Parses the details of an option from its 'galloName'.

    Args:
        gallo_name: The specific name string for the option.

    Returns:
        A dictionary with the underlying asset, option type, and strike price.
    """
    # Clean the input string by removing dots used as thousand separators
    cleaned_name = gallo_name.replace(".", "")

    # Regex to capture: 1) Ticker, 2) C or V, 3) Strike price with comma decimal separator
    match = re.match(r"([A-Z0-9]+)\s*\((C|V)\)\s*([\d,\.]+)", cleaned_name)

    if not match:
        return {}

    return {
        "underlying_asset": match.group(1),
        "option_type": "CALL" if match.group(2) == "C" else "PUT",
        "strike_price": float(match.group(3).replace(",", ".")),
    }
