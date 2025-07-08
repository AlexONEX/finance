import re


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
