import bisect
import datetime
import logging
import math
from collections import defaultdict

from rhizopus.primitives import Time, raise_for_time, raise_for_key, raise_for_value
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
)

logger = logging.getLogger(__name__)


class SeriesRecorder:
    """Records numerical observations (and their obs. time)"""

    _recent_observations: Dict[Union[str, Sequence[str]], float]
    _observed_times: List[Time]
    _observed_series: Dict[Union[str, Sequence[str]], Dict[Time, float]]

    def __init__(self):
        self._observed_times = []  # sorted list of all observation times
        self._observed_series = defaultdict(dict)
        self._recent_observations = {}

    def save(
        self,
        t: Time,
        key: Union[str, Sequence[str]],
        value: float,
        min_allowed: float = -1.0e24,
        max_allowed: float = 1e24,
    ):
        raise_for_time(t)
        raise_for_key(key)
        raise_for_value(key, value, min_allowed, max_allowed)

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
