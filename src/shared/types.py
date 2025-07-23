from typing import TypedDict
from datetime import datetime


class TransactionData(TypedDict):
    broker_transaction_id: str | None
    date: datetime
    ticker: str
    asset_type: str
    quantity: float
    currency: str
    price: float
    market_fees: float
    broker_fees: float
    taxes: float
