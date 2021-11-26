import sys

import pytest
import math
from rhizopus.primitives import (
    EPS_FINANCIAL,
    NEGLIGIBLE_POSITIVE_PORTFOLIO_NAV,
    float_almost_equal,
    float_seq_almost_equal,
)


def test_constants():
    assert 0.0 < NEGLIGIBLE_POSITIVE_PORTFOLIO_NAV < 1.0
    assert 0.0 < EPS_FINANCIAL < 1.0


def test_float_almost_equal_wrong_rel_error():
    with pytest.raises(ValueError):
        float_almost_equal(0.0, 0.0, err=-1.0)
    with pytest.raises(ValueError):
        float_almost_equal(0.0, 0.0, err=10.0)
    float_almost_equal(0.0, 0.0, err=0.0)

    with pytest.raises(ValueError):
        float_almost_equal(0.0, 0.0, err=math.nan)
    with pytest.raises(ValueError):
        float_almost_equal(0.0, 0.0, err=math.inf)
    with pytest.raises(ValueError):
        float_almost_equal(0.0, 0.0, err=-math.inf)


def test_float_almost_equal_spacial_floats():
    assert not float_almost_equal(math.nan, 0.0)
    assert not float_almost_equal(1.0, math.nan)
    assert not float_almost_equal(math.inf, 0.0)
    assert not float_almost_equal(1.0, -math.inf)
    assert not float_almost_equal(math.nan, math.inf)
    assert float_almost_equal(math.nan, math.nan)
    assert float_almost_equal(math.inf, math.inf)
    assert float_almost_equal(-math.inf, -math.inf)


def test_float_almost_equal_good():
    assert float_almost_equal(0.0, 0.0, 0.0)
    assert float_almost_equal(-0.0, 0.0, 0.0)
    assert float_almost_equal(-1.0, -1.0, 0.0)

    # different signs
    assert not float_almost_equal(-1e-24, 1e-24, err=1e-7)
    assert not float_almost_equal(-1e-24, 0.0, err=1e-7)

    # abs error
    assert float_almost_equal(1e-10, 2e-10, err=1e-8)
    assert float_almost_equal(-1e-10, -2e-10, err=1e-8)

    # rel error
    assert not float_almost_equal(1e-8 + 1e-10, 1e-8 + 2e-10, err=1e-8)
    assert not float_almost_equal(-1e-8 - 1e-10, -1e-8 - 2e-10, err=1e-8)

    assert float_almost_equal(0.0, 1e-7, err=1e-6)
    assert float_almost_equal(1e-7, 0.0, err=1e-6)
    assert float_almost_equal(1e8, 1e8 + 1, err=1e-7)

    assert float_almost_equal(1e24, 1e24)
    assert float_almost_equal(-1e24, -1e24)

    assert float_almost_equal(sys.float_info.max, sys.float_info.max)
    assert float_almost_equal(sys.float_info.min, sys.float_info.min)

    assert not float_almost_equal(0.0, sys.float_info.max)
    assert not float_almost_equal(sys.float_info.min, sys.float_info.max)
    assert not float_almost_equal(sys.float_info.max, sys.float_info.min)

    assert not float_almost_equal(1e-7, sys.float_info.max)
    assert not float_almost_equal(sys.float_info.max, 1e-7)

    assert float_almost_equal(0.0, sys.float_info.min, err=1e-8)
    assert not float_almost_equal(0.0, sys.float_info.min, err=0.0)


def test_float_array_almost_equal():
    assert float_seq_almost_equal([], [])
    assert float_seq_almost_equal([0.0], [0.0])

    assert not float_seq_almost_equal([1.0], [1.0, 0.0])
    assert not float_seq_almost_equal([1.0], [2.0])
