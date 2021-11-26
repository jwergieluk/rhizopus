import datetime
import logging
import math
import sys
from numbers import Real
from typing import Tuple, Union, Iterable, Sequence

Time = datetime.datetime
Observation = Tuple[Time, float]
Amount = Tuple[float, str]

# disable weights calculation for any portfolio with NAV below the following value:
NEGLIGIBLE_POSITIVE_PORTFOLIO_NAV = 1e-2
EPS_FINANCIAL = 1e-8
MIN_TIME, MAX_TIME = datetime.datetime(1970, 1, 1), datetime.datetime(2100, 1, 1)
DATE_FORMAT = '%Y-%m-%d'
MAX_INT_ID = 1000000000
MAX_KEY_LEN = 255
MIN_OBS_VALUE = -1e24
MAX_OBS_VALUE = 1e24
MULTI_KEY_SEP = '|'  # this character is used to convert Tuple[str] keys to str keys


logger = logging.getLogger(__name__)


def raise_for_time(t: Time) -> None:
    if type(t) != Time:
        raise TypeError(f'Wrong time provided: {t}')
    if t.tzinfo is not None:
        raise TypeError(f'Only native times with no timezone information are supported: {t}')
    if not (MIN_TIME <= t <= MAX_TIME):
        raise ValueError(
            f'Time outside of acceptable range: {t} must be in [{MIN_TIME}, {MAX_TIME})'
        )


def raise_for_key(key: Union[str, Iterable[str]]) -> None:
    if type(key) == tuple:
        if not (1 < len(key) < MAX_KEY_LEN):
            raise ValueError(f'Provided key has wrong length: {key}')
        if len(key) > 10:
            logger.warning(f'Unusually long key encountered: {key}')
        for k in key:
            if not (type(k) == str and 0 < len(k) < MAX_KEY_LEN):
                raise ValueError(f'Wrong key part detected: {k}')
            if MULTI_KEY_SEP in k:
                raise ValueError(
                    f'Key containing the separator char "{MULTI_KEY_SEP}" detected: {key}'
                )
            if not k.isprintable():
                logger.warning(f'Non-printable characters detected in key: "{k}"')
            if MULTI_KEY_SEP in k:
                raise ValueError(f'')
    elif type(key) == str:
        if not (0 < len(key) < MAX_KEY_LEN):
            raise ValueError(f'Passed key has wrong size: {key}')
        if MULTI_KEY_SEP in key:
            raise ValueError(f'Key containing the separator char "{MULTI_KEY_SEP}" detected: {key}')
        if not key.isprintable():
            logger.warning(f'Non-printable characters detected in key: "{key}"')
    else:
        raise TypeError(f'Passed key has wrong type: {key} ({type(key)})')


def raise_for_value(
    key: str,
    value: Real,
    min_allowed: float = -1e24,
    max_allowed: float = 1e24,
    allow_nans: bool = False,
) -> None:
    if not isinstance(value, Real):
        raise TypeError(f'Only Real values for {key} are allowed: {value}')
    value = float(value)
    if allow_nans:
        return
    if math.isnan(value):
        raise ValueError(f'NaN value {value} for {key}')
    if math.isfinite(min_allowed) and math.isfinite(max_allowed):
        if not math.isfinite(value):
            raise ValueError(f'Value {value} for {key} is not finite')
    if not (min_allowed <= value <= max_allowed):
        raise ValueError(
            f'Value {value} for {key} outside of acceptable range [{min_allowed} {max_allowed}]'
        )
    if not (min_allowed / 2 <= value <= max_allowed / 2):
        logger.warning(f'Unusually large value for {key} encountered: {value}')


def raise_for_str_id(sid: str) -> None:
    """Check string identifiers"""
    if not (type(sid) == str and 0 < len(sid) < MAX_KEY_LEN):
        raise TypeError(f'Wrong numeraire str passed: {sid}')
    if not sid.isprintable():
        logger.warning(f'Non-printable characters detected in "{sid}"')
    if len(sid) > int(MAX_KEY_LEN):
        logger.warning(f'Unusually long str id encountered: {sid}')


def raise_for_amount(amount: Amount) -> None:
    if not (type(amount) == tuple and len(amount) == 2):
        raise TypeError(f'Wrong amount type: {amount}')
    value, num = amount
    raise_for_str_id(num)
    raise_for_value(num, value)


def checked_time(t: Time) -> Time:
    raise_for_time(t)
    return t


def maybe_deserialize_time(t: Union[Time, str]) -> Time:
    """Deserialize a time-str in iso format if necessary"""
    if type(t) == Time:
        return checked_time(t)
    return checked_time(datetime.datetime.fromisoformat(t))


def maybe_serialize_time(t: Union[Time, str]) -> str:
    """Serialize time to iso format if necessary"""
    if type(t) == str:
        return t
    return t.isoformat(sep='T', timespec='microseconds')


def checked_str_id(num: str) -> str:
    raise_for_str_id(num)
    return num


def checked_int_id(value: int) -> int:
    if type(value) != int:
        raise TypeError(f'Int id has wrong type: {value} ({type(value)})')
    if not (0 <= value < MAX_INT_ID):
        raise ValueError(f'Int id value out of range: {value}')
    if value > int(MAX_INT_ID / 2):
        logger.warning(f'Unusually large int id encountered: {value}')
    return value


def checked_real(
    key: str,
    value: Real,
    min_allowed: float = -1e24,
    max_allowed: float = 1e24,
    allow_nans: bool = False,
) -> float:
    raise_for_value(key, value, min_allowed, max_allowed, allow_nans)
    return float(value)


def checked_amount(amount: Amount) -> Amount:
    raise_for_amount(amount)
    return amount


def float_almost_equal(a: float, b: float, err: float = 1e-8) -> bool:
    """Test approximate equality

    * Floats of with different sign are always not equal.
    * nan, inf, and -inf are all equal to itself.
    * The error is relative if either |a|>err or |b|>err, otherwise the error is absolute.

    There is a similar function in numpy, but it doesn't seem to be doing to right thing:
    https://numpy.org/doc/stable/reference/generated/numpy.testing.assert_array_almost_equal.html#numpy.testing.assert_array_almost_equal
    """
    if not (math.isfinite(err) and 0.0 <= err <= 1.0):
        raise ValueError(f'Wrong relative error: {err}')

    if not (math.isfinite(a) and math.isfinite(b)):
        if math.isnan(a) and math.isnan(b):
            return True
        return a == b
    if a == b:
        return True
    # different signs
    if min(a, b) < 0.0 <= max(a, b):
        return False
    if err <= sys.float_info.epsilon:
        return a == b

    # both positive or negative
    a, b = abs(a), abs(b)

    if min(a, b) < err:
        return abs(a - b) < err
    return abs(a - b) < max(err * max(a, b), sys.float_info.epsilon)


def float_seq_almost_equal(a: Sequence[float], b: Sequence[float], err: float = 1e-8) -> bool:
    if len(a) != len(b):
        return False
    return all(float_almost_equal(a[i], b[i], err) for i in range(len(a)))


def amount_almost_eq(amount0: Amount, amount1: Amount, eps: float = EPS_FINANCIAL) -> bool:
    return amount0[1] == amount1[1] and float_almost_equal(amount0[0], amount1[0], eps)
