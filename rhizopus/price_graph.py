import numbers
from functools import reduce
from operator import mul
from typing import Iterable, List, Mapping, Optional, Set, Tuple


def get_numeraires_from_prices(prices: Mapping[Tuple[str, str], numbers.Real]) -> Set[str]:
    return set([edge[0] for edge in prices] + [edge[1] for edge in prices])


def get_price_from_dict(
    prices: Mapping[Tuple[str, str], numbers.Real], num0: str, num1: str
) -> Optional[numbers.Real]:
    if num0 == num1:
        return 1.0
    key = (num0, num1)
    return prices.get(key, None)


def find_path(
    edges: Iterable[Tuple[str, str]],
    traversed_path: List[Tuple[str, str]],
    start_num: str,
    target_num: str,
    max_depth: int = 3,
):
    """Find a path in a graph (collection of edges)"""
    if max_depth == 0:
        return None
    visited_nodes = set()
    if len(traversed_path) > 0:
        for edge in traversed_path:
            visited_nodes.add(edge[0])
    for pair in edges:
        if pair[0] != start_num:
            continue
        if pair[1] == target_num:
            return traversed_path + [
                pair,
            ]
        if pair[1] in visited_nodes:
            continue
        full_path = find_path(
            edges,
            traversed_path
            + [
                pair,
            ],
            pair[1],
            target_num,
            max_depth - 1,
        )
        if full_path is not None:
            return full_path
    return None


def calc_path_price(
    prices: Mapping[Tuple[str, str], float], num0: Optional[str], num1: Optional[str]
) -> Optional[float]:
    """Given a price graph, calculate the exchange rate num0num1

    Example: for num0 == 'EUR' and and num1 == 'USD', the function will return the EURUSD rate.
    """
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


def calc_total_nav(
    prices: Mapping[Tuple[str, str], float],
    accounts: Mapping[str, Tuple[float, str]],
    target_num: str,
) -> Optional[float]:
    """Calculates total nav for given accounts in target_num"""
    running_sum = 0.0
    for acc, amount in accounts.items():
        val, num = amount
        p = calc_path_price(prices, num, target_num)
        if p is None:
            return p
        running_sum += val * calc_path_price(prices, num, target_num)
    return running_sum
