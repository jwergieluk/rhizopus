import pytest
from rhizopus.price_graph import find_path, calc_path_price


def test_find_path1():
    edges = [('EUR', 'USD'), ('USD', 'JPY'), ('JPY', 'CHF')]
    assert find_path(edges, [], 'EUR', 'CHF') == edges


def test_find_path2():
    edges = [('EUR', 'USD'), ('USD', 'JPY'), ('USD', 'EUR'), ('JPY', 'USD')]
    path = find_path(edges, [], 'JPY', 'EUR')
    assert len(path) > 1
    assert path[0][0] == 'JPY'
    assert path[-1][1] == 'EUR'
    for p1, p2 in zip(path[:-1], path[1:]):
        assert p1[1] == p2[0]


def test_path_price():
    spread = 0.95
    prices = {
        ('EUR', 'USD'): 1.2 * spread,
        ('USD', 'EUR'): spread * 1.0 / 1.2,
        ('XAU', 'USD'): spread * 1000.0 * 1.2,
        ('USD', 'XAU'): spread * 1.0 / (1000.0 * 1.2),
    }

    for num0, num1 in prices:
        assert calc_path_price(prices, num0, num1) == prices[(num0, num1)]

    assert calc_path_price(prices, 'XAU', 'EUR') == prices[('XAU', 'USD')] * prices[('USD', 'EUR')]
    assert calc_path_price(prices, 'EUR', 'XAU') == prices[('EUR', 'USD')] * prices[('USD', 'XAU')]
    assert calc_path_price(prices, 'XAU', 'ETH') is None


@pytest.mark.parametrize('chain_len', [5, 10, 30])
def test_long_path(chain_len: int):
    prices = {}
    for i in range(chain_len):
        prices[(f'N{i}', f'N{i+1}')] = 1.0
        prices[(f'N{i+1}', f'N{i}')] = 1.0

    assert calc_path_price(prices, f'N{chain_len}', 'N0') is None
    assert calc_path_price(prices, 'N0', f'N{chain_len}') is None


def test_one_way():
    prices = {
        ('EUR', 'USD'): 1.0,
        ('USD', 'XAU'): 1000.0,
    }

    assert calc_path_price(prices, 'EUR', 'XAU') == 1000.0
    assert calc_path_price(prices, 'XAU', 'EUR') is None
