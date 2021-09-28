import datetime
import itertools
import logging
import operator
from collections import deque, defaultdict
from typing import Optional, List, Union, Iterable, Tuple, Set, Dict

from rhizopus.broker import AbstractBrokerConn, BrokerError, BrokerState
from rhizopus.orders import (
    AddToAccountBalanceOrder,
    AddToVariableOrder,
    BackwardTransferOrder,
    Order,
)

SeriesStoreData = Dict[Tuple[str, str], List[Tuple[datetime.datetime, float]]]


class SeriesStoreBase:
    def __getitem__(self, key: Tuple[str, str]) -> Optional[List[Tuple[datetime.datetime, float]]]:
        raise NotImplementedError

    def __setitem__(self, edge: Tuple[str, str], series: Iterable[Tuple[datetime.datetime, float]]):
        raise NotImplementedError

    def edges(self) -> Iterable[Tuple[str, str]]:
        """Return all tradeable edges (numeraire pairs)"""
        raise NotImplementedError

    def vertices(self) -> Set[str]:
        raise NotImplementedError

    def get_min_time(self) -> datetime.datetime:
        """Return the earliest time for which we have at least one observation"""
        raise NotImplementedError

    def get_max_time(self) -> datetime.datetime:
        """Return the last time for which we have at least one observation"""
        raise NotImplementedError


class SeriesStoreFromDict(SeriesStoreBase):
    def __init__(self, init_data: SeriesStoreData):
        self._data = defaultdict(list)
        if init_data:
            for edge, series in init_data.items():
                self._data[edge] = sorted(series, key=operator.itemgetter(0))

    def __getitem__(self, key: Tuple[str, str]) -> Optional[List[Tuple[datetime.datetime, float]]]:
        return self._data.get(key)

    def __setitem__(self, edge: Tuple[str, str], series: Iterable[Tuple[datetime.datetime, float]]):
        self._data[edge] = sorted(series, key=operator.itemgetter(0))

    def edges(self) -> Iterable[Tuple[str, str]]:
        """Return all tradeable edges (numeraire pairs)"""
        return set(self._data.keys())

    def vertices(self) -> Set[str]:
        return set([edge[0] for edge in self.edges()] + [edge[1] for edge in self.edges()])

    def add_inverse_series(self) -> None:
        """For every numeraire pair (num0, num1) generate prices for (num1, num0) under zero-spread assumption"""
        inverse_ts = {}
        for k, v in self._data.items():
            key = (k[1], k[0])
            if key not in self._data.keys():
                inverse_ts[key] = [(t, 1.0 / w) for t, w in v]
        self._data.update(inverse_ts)

    def get_min_time(self) -> datetime.datetime:
        """Return the earliest time for which we have at least one observation"""
        return min(self._data[k][0][0] for k in self.edges())

    def get_max_time(self) -> datetime.datetime:
        """Return the last time for which we have at least one observation"""
        return max(self._data[k][-1][0] for k in self.edges())


class Filter:
    """Filter consumes an Order and produces arbitrary number of Orders"""

    def __call__(self, broker_state: BrokerState, order: Order) -> Union[Order, Iterable[Order]]:
        pass


class TransactionCostFilter(Filter):
    def __init__(self, cost_account: str, cost: float, cost_var: str, excluded_accounts: List[str]):
        self.cost_account = cost_account
        self.cost = cost
        self.cost_var = cost_var
        self.excluded_accounts = excluded_accounts

    def __call__(
        self, broker_state: BrokerState, order: BackwardTransferOrder
    ) -> Union[Order, Iterable[Order]]:
        if not isinstance(order, BackwardTransferOrder):
            return order
        if order.acc1 in self.excluded_accounts and order.acc0 in self.excluded_accounts:
            return order
        return (
            order,
            AddToAccountBalanceOrder(self.cost_account, -self.cost),
            AddToVariableOrder(self.cost_var, self.cost),
        )


class BrokerSimulator(AbstractBrokerConn):
    def __init__(
        self,
        time_series_store: SeriesStoreBase,
        filters: List[Filter],
        default_numeraire: str,
        start_time: datetime.datetime = datetime.datetime.min,
    ):
        self.filters = filters
        self.__default_numeraire = default_numeraire
        self.__start_time = start_time
        self.__prices = {}

        for num_pair in time_series_store.edges():
            num0 = num_pair[0]
            num1 = num_pair[1]
            series = time_series_store[num_pair]
            self.__prices[(num0, num1)] = dict(series)

        self.__time_grid = set()
        for key in self.__prices.keys():
            for times in self.__prices[key].keys():
                self.__time_grid.add(times)
        self.__time_grid = list(sorted(self.__time_grid))
        self.__time_index = 0
        self.__group_id = 0

        for self.__time_index in range(len(self.__time_grid)):
            if self.__time_grid[self.__time_index] >= self.__start_time:
                break

    def get_default_numeraire(self) -> Optional[str]:
        return self.__default_numeraire

    def next(self, broker_state: BrokerState) -> Optional[datetime.datetime]:
        self.__time_index += 1
        if len(self.__time_grid) < self.__time_index:
            raise BrokerError('Backtest end of time reached')
        if len(self.__time_grid) == self.__time_index:
            return None
        broker_state.time_index = self.__time_index
        broker_state.now = self.__time_grid[self.__time_index]
        broker_state.default_numeraire = self.__default_numeraire

        self.__update_current_prices(broker_state)
        try:
            self.__process_orders(broker_state)
        except BrokerError:
            raise
        return broker_state.now

    def __update_current_prices(self, broker_state: BrokerState) -> None:
        broker_state.current_prices.clear()
        for key in self.__prices.keys():
            if broker_state.now in self.__prices[key].keys():
                broker_state.current_prices[key] = self.__prices[key][broker_state.now]

    def __process_orders(self, broker_state: BrokerState) -> None:
        postponed_orders = []
        for order in sorted(broker_state.active_orders, key=lambda o: o.age):
            processed = order.execute(broker_state)
            time_str = broker_state.now.strftime('%Y-%m-%d %H:%M:%S')
            if processed:
                broker_state.executed_orders.append(order)
                logging.getLogger(__name__).info(
                    f"{time_str} T{broker_state.time_index} : Exec: {str(order)}"
                )
            else:
                order.age += 1
                if order.age % 128 == 0:
                    logging.getLogger(__name__).debug(
                        f"{time_str} T{broker_state.time_index}: Delay: {str(order)}"
                    )
                postponed_orders.append(order)

        broker_state.active_orders.clear()
        broker_state.active_orders.extend(postponed_orders)

    def fill_order(self, order, broker_state: BrokerState) -> None:
        """Applies filters to filled orders and appends the result to active_orders queue

        As the orders pass through the filter chain, they need to be dynamically added
        and removed from the active_orders queue. This is necessary because filters use that
        queue.
        """
        order.gid = self.__get_group_id()
        if not self.filters:
            broker_state.active_orders.append(order)
            return

        active_orders_snapshot = list(broker_state.active_orders)
        filter_input_orders = deque()
        filter_input_orders.append(order)
        for f in self.filters:
            filter_output_orders = deque()
            while filter_input_orders:
                input_order = filter_input_orders.popleft()
                broker_state.active_orders = list(
                    itertools.chain(
                        active_orders_snapshot,
                        filter_input_orders,
                        filter_output_orders,
                    )
                )
                output_orders = f(broker_state, input_order)

                if not output_orders:
                    continue
                try:
                    filter_output_orders.extend(output_orders)
                except TypeError:
                    filter_output_orders.append(output_orders)
            filter_input_orders = filter_output_orders
        broker_state.active_orders = deque(active_orders_snapshot, maxlen=1000)
        broker_state.active_orders.extend(filter_input_orders)

    def __get_group_id(self) -> int:
        self.__group_id += 1
        return self.__group_id
