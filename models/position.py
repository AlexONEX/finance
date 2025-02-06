from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional

@dataclass
class Position:
    ticker: str
    shares: int
    purchase_price: Decimal
    purchase_date: datetime
    purchase_ccl: Optional[Decimal] = None
    ratio: Optional[int] = None