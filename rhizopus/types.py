import datetime
import math
from numbers import Real
from typing import Tuple

TTime = datetime.datetime
min_time, max_time = datetime.datetime.min, datetime.datetime.max
Observation = Tuple[TTime, Real]
Amount = Tuple[float, str]


def checked_amount(amount: Amount) -> Amount:
    if not (type(amount) == tuple and len(amount) == 2):
        raise ValueError(f'Wrong amount: {amount}')
    value, num = amount
    if not (isinstance(value, float) and math.isfinite(value)):
        raise ValueError(f'Wrong amount value: {amount}')
    if not (type(num) == str and num):
        raise ValueError(f'Wrong amount numeraire: {amount}')
    return amount
