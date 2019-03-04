import numbers
from functools import reduce
from operator import mul
from typing import Mapping, Optional, Tuple, Set, Sequence, MutableMapping, Iterable, List, Any


def get_numeraires_from_prices(prices: Mapping[Tuple[str, str], numbers.Real]) -> Set[str]:
    return set([edge[0] for edge in prices] + [edge[1] for edge in prices])


def get_price_from_dict(prices: Mapping[Tuple[str, str], numbers.Real], num0: str, num1: str) -> Optional[numbers.Real]:
    if num0 == num1:
        return 1.0
    key = (num0, num1)
    if key in prices.keys():
        return prices[key]
    return None


def add_inverse_series(ts: MutableMapping[Tuple[str, str], Sequence[Tuple[Any, numbers.Real]]]):
    """ For every numeraire pair (num0, num1) generate prices for (num1, num0) under zero-spread assumption"""
    inverse_ts = {}
    for k, v in ts.items():
        key = (k[1], k[0])
        if key not in ts.keys():
            inverse_ts[key] = [(t, 1.0/w) for t, w in v]
    ts.update(inverse_ts)


def find_path(edges: Iterable[Tuple[str, str]], traversed_path: List[Tuple[str, str]],
              start_num: str, target_num: str, max_depth: int = 3):
    if max_depth == 0:
        return traversed_path
    visited_nodes = set()
    if len(traversed_path) > 0:
        for edge in traversed_path:
            visited_nodes.add(edge[0])
    for pair in edges:
        if pair[0] != start_num:
            continue
        if pair[1] == target_num:
            return traversed_path + [pair, ]
        if pair[1] in visited_nodes:
            continue
        full_path = find_path(edges, traversed_path + [pair, ], pair[1], target_num, max_depth - 1)
        if full_path is not None:
            return full_path
    return None


def calc_path_price(prices: Mapping[Tuple[str, str], float],
                    num0: Optional[str], num1: Optional[str]) -> Optional[float]:
    if num0 is None or num1 is None:
        return None
    if num0 == num1:
        return 1.0
    key = (num0, num1)
    if key in prices.keys():
        return prices[key]
    path = find_path(prices.keys(), [], num0, num1)
    if path is None:
        return None
    return reduce(mul, [prices[pair] for pair in path])


def calc_total_nav(prices: Mapping[Tuple[str, str], float], accounts: Mapping[str, Tuple[float, str]],
                   target_num: str) -> Optional[float]:
    """ Calculates total nav for given accounts in target_num """
    running_sum = 0.0
    for acc, amount in accounts.items():
        val, num = amount
        p = calc_path_price(prices, num, target_num)
        if p is None:
            return p
        running_sum += val*calc_path_price(prices, num, target_num)
    return running_sum
