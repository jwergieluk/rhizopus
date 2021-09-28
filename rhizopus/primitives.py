import datetime
import math
from typing import Tuple, Union, Iterable

Time = datetime.datetime
Observation = Tuple[Time, float]
Amount = Tuple[float, str]

MIN_TIME, MAX_TIME = datetime.datetime(1970, 1, 1), datetime.datetime(2100, 1, 1)


def checked_amount(amount: Amount) -> Amount:
    if not (type(amount) == tuple and len(amount) == 2):
        raise ValueError(f'Wrong amount: {amount}')
    value, num = amount
    if not (isinstance(value, float) and math.isfinite(value)):
        raise ValueError(f'Wrong amount value: {amount}')
    if not (type(num) == str and num):
        raise ValueError(f'Wrong amount numeraire: {amount}')
    return amount


def raise_for_time(t: Time) -> None:
    if type(t) != Time:
        raise ValueError(f'Wrong time provided: {t}')
    if t.tzinfo is not None:
        raise ValueError(f'Only native times with no timezone information are supported: {t}')
    if not (MIN_TIME < t < MAX_TIME):
        raise ValueError(
            f'Time outside of acceptable range: {t} must be in [{MIN_TIME}, {MAX_TIME})'
        )


def raise_for_key(key: Union[str, Iterable[str]]) -> None:
    if isinstance(key, tuple):
        if not (1 < len(key) < 256 and all(isinstance(x, str) and 0 < len(x) < 256 for x in key)):
            raise ValueError(f'Wrong key provided: {key}')
    elif isinstance(key, str):
        if not (0 < len(key) < 256):
            raise ValueError(f'Passed key has wrong size: {key}')
    else:
        raise ValueError(f'Passed key has wrong type: {key}')


def raise_for_value(
    key: str, value: float, min_allowed: float = -1e24, max_allowed: float = 1e24
) -> None:
    if type(value) != float:
        raise ValueError(f'Only float values for {key} are allowed: {value}')
    if not (math.isfinite(value) and min_allowed <= value <= max_allowed):
        raise ValueError(
            f'Value {value} for {key} outside of acceptable range [{min_allowed} {max_allowed}]'
        )
