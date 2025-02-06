from datetime import datetime
from decimal import Decimal
from models.position import Position

CEDEAR_POSITIONS = {
    'BABAD': Position('BABAD', 55, Decimal('11.498'), datetime(2024, 1, 1), Decimal('1000'), 9),
    'BBDD': Position('BBDD', 797, Decimal('2.182'), datetime(2024, 1, 1), Decimal('1000'), 1),
    'VISTD': Position('VISTD', 25, Decimal('18.718'), datetime(2024, 1, 1), Decimal('1000'), 3),
    'VALED': Position('VALED', 41, Decimal('4.817'), datetime(2024, 1, 1), Decimal('1000'), 2),
    'NIOD': Position('NIOD', 500, Decimal('1.151'), datetime(2024, 1, 1), Decimal('1000'), 4),
    'NUD': Position('NUD', 91, Decimal('6.871'), datetime(2024, 1, 1), Decimal('1000'), 2),
    'ADGO': Position('ADGO', 3, Decimal('10.107'), datetime(2024, 1, 1), Decimal('1000'), 1)
}

BOND_POSITIONS = {
    'AE38D': Position('AE38D', 3866, Decimal('0.7077'), datetime(2024, 1, 1), Decimal('1000'))
}
