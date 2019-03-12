import datetime
from typing import Tuple
from numbers import Real

TTime = datetime.datetime
min_time, max_time = datetime.datetime.min, datetime.datetime.max
Observation = Tuple[TTime, Real]
Amount = Tuple[Real, str]
