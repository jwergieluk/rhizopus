import datetime
import logging
import math
from numbers import Real
from typing import Tuple, Union, Iterable

Time = datetime.datetime
Observation = Tuple[Time, float]
Amount = Tuple[float, str]

MIN_TIME, MAX_TIME = datetime.datetime(1970, 1, 1), datetime.datetime(2100, 1, 1)


logger = logging.getLogger(__name__)


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
    if type(key) == tuple:
        if not (1 < len(key) < 256):
            raise ValueError(f'Provided key has wrong length: {key}')
        for k in key:
            if not (type(k) == str and 0 < len(k) < 256):
                raise ValueError(f'Wrong key part detected: {k}')
            if not k.isprintable():
                logger.warning(f'Non-printable characters detected in key: "{k}"')
    elif type(key) == str:
        if not (0 < len(key) < 256):
            raise ValueError(f'Passed key has wrong size: {key}')
        if not key.isprintable():
            logger.warning(f'Non-printable characters detected in key: "{key}"')
    else:
        raise ValueError(f'Passed key has wrong type: {key}')


def raise_for_value(
    key: str, value: Real, min_allowed: float = -1e24, max_allowed: float = 1e24
) -> None:
    if not isinstance(value, Real):
        raise ValueError(f'Only Real values for {key} are allowed: {value}')
    value = float(value)
    if not (math.isfinite(value) and min_allowed <= value <= max_allowed):
        raise ValueError(
            f'Value {value} for {key} outside of acceptable range [{min_allowed} {max_allowed}]'
        )


def raise_for_str_id(num: str) -> None:
    """Check string identifiers"""
    if not (type(num) == str and 0 < len(num) < 256):
        raise ValueError(f'Wrong numeraire str passed: {num}')
    if not num.isprintable():
        logger.warning(f'Non-printable characters detected in "{num}"')


def raise_for_amount(amount: Amount) -> None:
    if not (type(amount) == tuple and len(amount) == 2):
        raise ValueError(f'Wrong amount: {amount}')
    value, num = amount
    raise_for_str_id(num)
    raise_for_value(num, value)


def checked_amount(amount: Amount) -> Amount:
    raise_for_amount(amount)
    return amount


def checked_str_id(num: str) -> str:
    raise_for_str_id(num)
    return num


def checked_value(
    key: str, value: Real, min_allowed: float = -1e24, max_allowed: float = 1e24
) -> float:
    raise_for_value(key, value, min_allowed, max_allowed)
    return float(value)
