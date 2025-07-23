import pandas as pd
from typing import Dict, Any, Optional
from src.shared.types import TransactionData


def parse_ieb_movement(movement: Dict[str, Any]) -> Optional[TransactionData]:
    """
    Parses a movement from the IEB API into a TransactionData object.
    If the movement cannot be parsed, returns None.
    """
    op_type_map = {
        "CPRA": "BUY",
        "VTAS": "SELL",
        "DIV": "DIVIDEND",
    }

    operation = movement.get("operation")
    if operation not in op_type_map:
        return None

    asset_type = "ACCION"

    try:
        quantity = abs(float(movement["amount"]))
        if quantity <= 0:
            return None

        parsed: TransactionData = {
            "op_type": op_type_map[operation],
            "broker_transaction_id": movement.get("documentKey"),
            "date": pd.to_datetime(movement.get("operationDate")),
            "ticker": movement.get("especie"),
            "quantity": quantity,
            "price": float(movement.get("price", 0)),
            "asset_type": asset_type,
            "currency": "ARS",  # Asumimos ARS por defecto.
            "market_fees": 0.0,  # No disponible en este endpoint.
            "broker_fees": 0.0,  # No disponible en este endpoint.
            "taxes": 0.0,  # No disponible en este endpoint.
        }
        return parsed
    except (ValueError, TypeError) as e:
        print(f"Error parseando movimiento {movement.get('documentKey')}: {e}")
        return None
