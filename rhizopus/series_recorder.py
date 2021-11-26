import bisect
import datetime
import logging
from collections import defaultdict
from types import MappingProxyType
from typing import (
    Dict,
    KeysView,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
    Any,
)

from rhizopus.primitives import (
    Time,
    raise_for_time,
    raise_for_key,
    checked_real,
    raise_for_value,
    MIN_OBS_VALUE,
    MAX_OBS_VALUE,
    float_almost_equal,
    EPS_FINANCIAL,
    MULTI_KEY_SEP,
    maybe_serialize_time,
)

logger = logging.getLogger(__name__)


class SeriesRecorder:
    """Records numerical observations and their observation times"""

    _observed_times: List[Time]
    _observed_series: Dict[Union[str, Sequence[str]], Dict[Time, float]]
    _recent_observations: Dict[Union[str, Sequence[str]], float]

    def __init__(
        self,
        observed_times: Optional[List[Time]] = None,
        observed_series: Optional[Dict[Union[str, Sequence[str]], Dict[Time, float]]] = None,
        recent_observations: Dict[Union[str, Sequence[str]], float] = None,
    ):
        # sorted list of all observation times
        self._observed_times = observed_times if observed_times else []
        self._observed_series = defaultdict(dict, observed_series if observed_series else {})
        self._recent_observations = recent_observations if recent_observations else {}

        for key, value in self._recent_observations.items():
            raise_for_key(key)
            key_str = key if isinstance(key, str) else '_'.join(key)
            raise_for_value(key_str, value)
        for key in self._observed_series:
            raise_for_key(key)

    def save(
        self,
        t: Time,
        key: Union[str, Sequence[str]],
        value: float,
        min_allowed: float = MIN_OBS_VALUE,
        max_allowed: float = MAX_OBS_VALUE,
        allow_nans: bool = False,
    ):
        raise_for_time(t)
        raise_for_key(key)
        value = checked_real(key, value, min_allowed, max_allowed, allow_nans)

        if t in self._observed_series[key].keys():
            logger.warning(
                f'Updated observation of {key} for t {t}: {self._observed_series[key][t]} -> {value}'
            )
        else:
            i = bisect.bisect_left(self._observed_times, t)
            if i == len(self._observed_times):
                self._observed_times.append(t)
            if i < len(self._observed_times) and self._observed_times[i] != t:
                self._observed_times.insert(i, t)
        self._observed_series[key][t] = value

        if max(self._observed_series[key].keys()) == t:
            self._recent_observations[key] = value

    def get_dict(self, key: Union[str, Sequence[str]]) -> Optional[Mapping[Time, float]]:
        if key not in self._observed_series.keys():
            return None
        return MappingProxyType(self._observed_series[key])

    def _obs_pair_generator(
        self,
        key: Union[str, Sequence[str]],
        starting_with: Time = datetime.datetime.min,
        ending_not_later_than: Time = datetime.datetime.max,
    ):
        series = self._observed_series[key]
        for i in range(
            bisect.bisect(self._observed_times, starting_with),
            len(self._observed_times),
        ):
            t = self._observed_times[i]
            if t in series.keys() and t <= ending_not_later_than:
                x = series[t]
                yield t, x

    def get_list_of_pairs(
        self,
        key: Union[str, Sequence[str]],
        starting_with: Time = datetime.datetime.min,
        ending_not_later_than: Time = datetime.datetime.max,
    ) -> Optional[Sequence[Tuple[Time, float]]]:
        if key not in self._observed_series.keys():
            return None
        return list(self._obs_pair_generator(key, starting_with, ending_not_later_than))

    def get_t_x(
        self,
        key: Union[str, Sequence[str]],
        starting_with: Time = datetime.datetime.min,
        ending_not_later_than: Time = datetime.datetime.max,
    ) -> Tuple[Sequence[Time], Sequence[float]]:
        if key not in self._observed_series.keys():
            return [], []
        t_list, x_list = [], []
        for t, x in self._obs_pair_generator(key, starting_with, ending_not_later_than):
            t_list.append(t)
            x_list.append(x)
        return t_list, x_list

    def get_recent_observations(self) -> Mapping[Union[str, Sequence[str]], float]:
        return MappingProxyType(self._recent_observations)

    def keys(self) -> KeysView[Union[str, Sequence[str]]]:
        return self._observed_series.keys()

    def times(self) -> List[Time]:
        return self._observed_times

    def to_json(self) -> Dict[str, Any]:
        series = {}
        for key, t_v_dict in self._observed_series.items():
            str_key = key if isinstance(key, str) else MULTI_KEY_SEP.join(key)
            series[str_key] = {maybe_serialize_time(t): v for t, v in t_v_dict.items()}
        recent_observations = {}
        for key, value in self._recent_observations.items():
            str_key = key if isinstance(key, str) else MULTI_KEY_SEP.join(key)
            recent_observations[str_key] = value
        return {
            'observed_times': [maybe_serialize_time(t) for t in self._observed_times],
            'observed_series': series,
            'recent_observations': recent_observations,
        }

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> 'SeriesRecorder':
        observed_times = [datetime.datetime.fromisoformat(t) for t in data['observed_times']]
        observed_series = {}
        for str_key, series in data['observed_series'].items():
            key = tuple(str_key.split(MULTI_KEY_SEP)) if MULTI_KEY_SEP in str_key else str_key
            observed_series[key] = {
                datetime.datetime.fromisoformat(t): v for t, v in series.items()
            }
        recent_observations = {}
        for str_key, price in data['recent_observations'].items():
            key = tuple(str_key.split(MULTI_KEY_SEP)) if MULTI_KEY_SEP in str_key else str_key
            recent_observations[key] = float(price)
        return SeriesRecorder(observed_times, observed_series, recent_observations)

    def __eq__(self, other: 'SeriesRecorder') -> bool:
        if not (
            self._observed_times == other._observed_times
            and set(self._recent_observations) == set(other._recent_observations)
            and set(self._observed_series) == set(self._observed_series)
        ):
            return False
        for key in self._recent_observations:
            if not float_almost_equal(
                self._recent_observations[key], other._recent_observations[key], EPS_FINANCIAL
            ):
                return False
        for key in self._observed_series:
            series0 = self._observed_series[key]
            series1 = other._observed_series[key]
            if set(series0) != set(series1):
                return False
            for t in series0:
                if not float_almost_equal(series0[t], series1[t], EPS_FINANCIAL):
                    return False
        return True
