import pandas as pd
from typing import Dict, Any, Optional
from src.shared.types import TransactionData


def parse_ieb_movement(movement: Dict[str, Any]) -> Optional[TransactionData]:
    """
    Convierte un movimiento del historial de IEB a nuestro formato de transacción genérico.
    Devuelve None si el movimiento no es una compra o venta soportada.
    """
    op_type_map = {
        "CPRA": "BUY",
        "VTAS": "SELL",
        "DIV": "DIVIDEND",
    }

    operation = movement.get("operation")
    if operation not in op_type_map:
        return None

    # El nuevo endpoint no provee información detallada de fees o asset_type.
    # Usamos valores por defecto y lo marcamos como una limitación a tener en cuenta.
    # TODO: Encontrar una forma de enriquecer el `asset_type` si es necesario.
    asset_type = "ACCION"  # Asumimos 'ACCION' por defecto.

    try:
        quantity = abs(float(movement["amount"]))
        if quantity <= 0:
            return None  # Ignorar movimientos sin cantidad

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
