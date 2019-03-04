import datetime
import itertools
from collections import deque
import logging
from . import BrokerState, BrokerError, Order, BackwardTransferOrder, AddToAccountBalanceOrder, AddToVariableOrder


class Filter:
    """ Filter consumes an Order and produces arbitrary number of Orders """

    def __call__(self, broker_state: BrokerState, order: Order):
        pass


class TransactionCostFilter(Filter):
    def __init__(self, cost_account: str, cost: float, cost_var: str, excluded_accounts: list):
        self.cost_account = cost_account
        self.cost = cost
        self.cost_var = cost_var
        self.excluded_accounts = excluded_accounts

    def __call__(self, broker_state: BrokerState, order: BackwardTransferOrder):
        if not isinstance(order, BackwardTransferOrder):
            return order
        if order.acc1 in self.excluded_accounts and order.acc0 in self.excluded_accounts:
            return order
        return (order, AddToAccountBalanceOrder(self.cost_account, -self.cost),
                AddToVariableOrder(self.cost_var, self.cost))


class BrokerSimulator:
    def __init__(self, time_series_store, filters: list, default_numeraire: str,
                 start_time: datetime.datetime = datetime.datetime.min):
        self.__filters = filters
        self.__default_numeraire = default_numeraire
        self.__start_time = start_time
        self.__prices = {}

        for num_pair in time_series_store.keys():
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

    def next(self, broker_state: BrokerState):
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

    def __update_current_prices(self, broker_state: BrokerState):
        broker_state.current_prices.clear()
        for key in self.__prices.keys():
            if broker_state.now in self.__prices[key].keys():
                broker_state.current_prices[key] = self.__prices[key][broker_state.now]

    def __process_orders(self, broker_state: BrokerState):
        postponed_orders = []
        for order in sorted(broker_state.active_orders, key=lambda o: o.age):
            processed = order.execute(broker_state)
            time_str = broker_state.now.strftime('%H:%M:%S')
            if processed:
                broker_state.executed_orders.append(order)
                logging.getLogger(__name__).debug(f"{time_str} T{broker_state.time_index} : Exec: {str(order)}")
            else:
                order.age += 1
                if order.age % 128 == 0:
                    logging.getLogger(__name__).debug(
                        f"{time_str} T{broker_state.time_index}: Delay: {str(order)}")
                postponed_orders.append(order)

        broker_state.active_orders.clear()
        broker_state.active_orders.extend(postponed_orders)

    def fill_order(self, order, broker_state: BrokerState):
        """ Applies filters to filled orders and appends the result to active_orders queue

            As the orders pass through the filter chain, they need to be dynamically added
            and removed from the active_orders queue. This is necessary because filters use that
            queue.
        """
        order.gid = self.__get_group_id()
        if self.__filters is None or len(self.__filters) == 0:
            broker_state.active_orders.append(order)
            return

        active_orders_snapshot = list(broker_state.active_orders)
        filter_input_orders = deque()
        filter_input_orders.append(order)
        for f in self.__filters:
            filter_output_orders = deque()
            while filter_input_orders:
                input_order = filter_input_orders.popleft()
                broker_state.active_orders = list(itertools.chain(active_orders_snapshot,
                                                                  filter_input_orders,
                                                                  filter_output_orders))
                output_orders = f(broker_state, input_order)

                if output_orders is None:
                    continue
                try:
                    filter_output_orders.extend(output_orders)
                except TypeError:
                    filter_output_orders.append(output_orders)
            filter_input_orders = filter_output_orders
        broker_state.active_orders = deque(active_orders_snapshot, maxlen=1000)
        broker_state.active_orders.extend(filter_input_orders)

    def __get_group_id(self):
        self.__group_id += 1
        return self.__group_id
