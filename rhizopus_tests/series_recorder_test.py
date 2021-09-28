import datetime
import math
import random
import pytest

from rhizopus.series_recorder import SeriesRecorder


def d2s(series: dict):
    return [(t, series[t]) for t in sorted(set(series.keys()))]


def some_t_x(t0, n: int = 50):
    """Return datetime list t and float list x. The list t is increasing."""
    n = random.randint(n, 5 * n)
    t = set(
        t0 + datetime.timedelta(seconds=int(random.normalvariate(0.0, 2.0 * n))) for _ in range(n)
    )
    x = [random.expovariate(0.1) for _ in range(len(t))]
    t = list(sorted(t))
    return t, x


def some_observation_pairs(t0, n: int = 50):
    """Returns a list of some (datetime, float) pairs. Not sorted by time."""
    t, x = some_t_x(t0, n)
    random.shuffle(t)
    return list(zip(t, x))


def test_simple1():
    t0 = datetime.datetime.utcnow()
    s1, s2, s3 = (some_observation_pairs(t0) for _ in range(3))

    rec = SeriesRecorder()
    for t, x in s1:
        rec.save(t, 's1', x)
    for t, x in s2:
        rec.save(t, 's2', x)
    for t, x in s3:
        rec.save(t, 's3', x)

    for t, x in s1:
        rec.save(t, 's1', x)

    s1, s2, s3 = dict(s1), dict(s2), dict(s3)

    assert rec.get_dict('s0') is None
    assert rec.get_dict('s1') == s1
    assert rec.get_dict('s2') == s2
    assert rec.get_dict('s3') == s3

    assert set(rec.keys()) == {'s1', 's2', 's3'}
    assert rec.times() == list(sorted(s1.keys() | s2.keys() | s3.keys()))
    assert rec.get_recent_observations() == {
        's1': s1[max(s1.keys())],
        's2': s2[max(s2.keys())],
        's3': s3[max(s3.keys())],
    }

    assert rec.get_list_of_pairs('s1') == d2s(s1)
    assert rec.get_list_of_pairs('s2') == d2s(s2)
    assert rec.get_list_of_pairs('s3') == d2s(s3)
    assert min(dict(rec.get_list_of_pairs('s3', t0)).keys()) >= t0

    assert rec.get_t_x('s0') == ([], [])
    assert rec.get_t_x('s1') == ([x[0] for x in d2s(s1)], [x[1] for x in d2s(s1)])
    assert rec.get_t_x('s2') == ([x[0] for x in d2s(s2)], [x[1] for x in d2s(s2)])
    assert rec.get_t_x('s3') == ([x[0] for x in d2s(s3)], [x[1] for x in d2s(s3)])
    assert min(rec.get_t_x('s1', t0)[0]) >= t0


def test_tzinfo0():
    wrong_times = []
    t = datetime.datetime.utcnow()
    t = datetime.datetime.combine(
        t.date(), t.time(), tzinfo=datetime.timezone(offset=datetime.timedelta(hours=1), name='CET')
    )
    wrong_times.append(t)

    t = datetime.datetime.utcnow()
    t = datetime.datetime.combine(t.date(), t.time(), tzinfo=datetime.timezone.utc)
    wrong_times.append(t)

    series_recorder = SeriesRecorder()
    for t in wrong_times:
        with pytest.raises(ValueError):
            series_recorder.save(t, 'key0', 0.1)

    series_recorder.save(datetime.datetime.utcnow(), 'key', 2.33)


@pytest.mark.parametrize(
    'key', ['', 0, ('key', 0), ('key', 0.1), ('key', ''), ('key',), ('key', None), (None,)]
)
def test_wrong_key(key):
    rec = SeriesRecorder()
    with pytest.raises(ValueError):
        rec.save(datetime.datetime.utcnow(), key, 0.01)


@pytest.mark.parametrize('value', [None, 0, '0', '', math.nan, math.inf, -math.inf])
def test_wrong_value(value):
    rec = SeriesRecorder()
    with pytest.raises(ValueError):
        rec.save(datetime.datetime.utcnow(), 'key', value)


@pytest.mark.parametrize(
    't',
    [
        datetime.datetime.utcnow().date(),
        datetime.datetime.utcnow().time(),
        0.0,
        0,
        None,
        '',
        datetime.datetime(1965, 1, 1),
        datetime.datetime(2100, 1, 1),
    ],
)
def test_wrong_time(t):
    rec = SeriesRecorder()
    with pytest.raises(ValueError):
        rec.save(t, 'key', 1.0)
