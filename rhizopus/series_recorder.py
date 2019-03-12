import datetime
from types import MappingProxyType
from typing import Optional, Dict, Tuple, Sequence, Union, KeysView, List, Mapping
import numbers
from collections import defaultdict
import math
import logging
import bisect
from . import TTime


class SeriesRecorder:
    """ Records numerical observations (and their obs. time) """
    _recent_observations: Dict[Union[str, Sequence[str]], float]
    _observed_times: List[TTime]
    _observed_series: Dict[Union[str, Sequence[str]], Dict[TTime, float]]

    def __init__(self):
        self._observed_times = []  # sorted list of all observation times
        self._observed_series = defaultdict(dict)
        self._recent_observations = {}

    def save(self, t: TTime, key: Union[str, Sequence[str]], value: float,
             min_allowed: float = -1.0e24, max_allowed: float = 1e24):
        assert t is not None and key is not None and 0 < len(key) < 256 and all(0 < len(x) < 256 for x in key)
        assert isinstance(value, numbers.Number) or value is None, f'key {key}, value {value}'

        if value is None:
            return
        assert not math.isnan(value) and min_allowed < value < max_allowed

        if t in self._observed_series[key].keys():
            logging.info(f'Update observation of {key} @{t}: {self._observed_series[key][t]} -> {value}')
        else:
            i = bisect.bisect_left(self._observed_times, t)
            if i == len(self._observed_times):
                self._observed_times.append(t)
            if i < len(self._observed_times) and self._observed_times[i] != t:
                self._observed_times.insert(i, t)
        self._observed_series[key][t] = value

        if max(self._observed_series[key].keys()) == t:
            self._recent_observations[key] = value

    def get_dict(self, key: Union[str, Sequence[str]]) -> Optional[Mapping[TTime, float]]:
        if key not in self._observed_series.keys():
            return None
        return MappingProxyType(self._observed_series[key])

    def get_list_of_pairs(self, key: Union[str, Sequence[str]], starting_with: TTime = datetime.datetime.min
                          ) -> Optional[Sequence[Tuple[TTime, float]]]:
        if key not in self._observed_series.keys():
            return None
        ret = []
        series = self._observed_series[key]
        for i in range(bisect.bisect(self._observed_times, starting_with), len(self._observed_times)):
            t = self._observed_times[i]
            if t in series.keys():
                x = series[t]
                ret.append((t, x))
        return ret

    def get_t_x(self, key: Union[str, Sequence[str]], starting_with: TTime = datetime.datetime.min
                ) -> Tuple[Sequence[TTime], Sequence[float]]:
        if key not in self._observed_series.keys():
            return [], []
        series = self._observed_series[key]
        t_list, x_list = [], []
        for i in range(bisect.bisect(self._observed_times, starting_with), len(self._observed_times)):
            t = self._observed_times[i]
            if t in series.keys():
                t_list.append(t)
                x_list.append(series[t])
        return t_list, x_list

    def get_recent_observations(self) -> Mapping[Union[str, Sequence[str]], float]:
        return MappingProxyType(self._recent_observations)

    def keys(self) -> KeysView[Union[str, Sequence[str]]]:
        return self._observed_series.keys()

    def times(self) -> List[TTime]:
        return self._observed_times
