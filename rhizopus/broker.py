import collections
import logging
import math
from abc import ABC
from collections import deque
import datetime
from enum import Enum, auto
from types import MappingProxyType
from typing import Dict, KeysView, List, Mapping, Optional, Tuple, Union, Iterable

from rhizopus.price_graph import calc_path_price, get_price_from_dict, price_graph_is_full
from rhizopus.primitives import Time, Amount, raise_for_time, raise_for_str_id

logger = logging.getLogger(__name__)
# disable weights calculation for any portfolio with NAV below the following value:
NEGLIGIBLE_POSITIVE_PORTFOLIO_NAV = 1e-2


class BrokerError(Exception):
    pass


class BrokerConnectionError(BrokerError):
    pass


class BrokerResponseError(BrokerError):
    pass


class BrokerStateError(BrokerError):
    pass


class BrokerState:
    """Encapsulates the state of the abstract broker.

    This class is passed to concrete brokers and each one of those maps their internal state to the fields
    of this class. We assume the following:
    * The types of the fields correspond to the type annotations as specified below, at any time.
    """

    variables: Dict[str, Union[float, str]]
    accounts: Dict[str, Amount]
    current_prices: Dict[Tuple[str, str], float]
    recent_prices: Dict[Tuple[str, str], float]
    default_numeraire: str
    now: Optional[Time]
    time_index: int

    def __init__(
        self,
        default_numeraire: str,
        accounts: Optional[Dict[str, Amount]] = None,
        variables: Optional[Dict[str, Union[float, str]]] = None,
    ):
        if not default_numeraire:
            raise BrokerStateError("Numeraire has to be a non-empty string")
        self.default_numeraire = default_numeraire
        self.accounts = dict(accounts) if accounts else {}
        self.variables = dict(variables) if variables else {}
        for acc in self.accounts:
            raise_for_str_id(acc)
        for v in self.variables:
            raise_for_str_id(v)
        self.current_prices = {}
        self.recent_prices = {}

        self.now = None
        self.time_index = 0
        self.active_orders = deque(maxlen=50000)
        self.executed_orders = deque(maxlen=100000)
        self.rejected_orders = deque(maxlen=5000)

    def check(self):
        """Self-check

        More checks to implement:
        * Check if price graphs are arbitrage free
        * Add properties for default_numeraire, now, and time_index to make sure they are set properly. This is
          cheaper than checking every iteration.
        """
        if not (type(self.default_numeraire) == str and self.default_numeraire):
            raise BrokerStateError(f'Wrong default numeraire: {self.default_numeraire}')
        if not (type(self.time_index) == int and self.time_index >= 0):
            raise BrokerStateError(f'Wrong time index: {self.time_index}')

        raise_for_time(self.now)


class OrderStatus(Enum):
    ACTIVE = auto()
    EXECUTED = auto()
    REJECTED = auto()


class Order:
    """Represents an order executed by a Broker instance

    An order is not allowed to have variable state which must be stored in BrokerState.
    The execute() method is invoked only by the BrokerSimulator.
    """

    def __init__(self, gid: int = 0):
        self.age = 0
        self.status = OrderStatus.ACTIVE
        self.status_time_stamp = (
            datetime.datetime.min
        )  # time stamp at which the current status was set
        self.status_comment: str = ''
        self.transaction_id = None
        self.gid = gid

    def execute(self, broker_state: BrokerState) -> OrderStatus:
        """Order execution in the simulation environment

        Returns the order status resulting from the execution attempt.
        """
        raise NotImplementedError

    def set_status(
        self, new_status: OrderStatus, time_stamp: datetime.datetime, comment: str = ''
    ) -> OrderStatus:
        if self.status != new_status and (
            self.status in (OrderStatus.EXECUTED, OrderStatus.REJECTED)
        ):
            raise ValueError(f'Forbidden status update requested: {self.status} -> {new_status}')
        self.status = new_status
        self.status_time_stamp = time_stamp
        self.status_comment = comment
        return self.status

    def __repr__(self):
        return f"{self.__class__.__name__}()"

    def __str__(self):
        return f"{self.__class__.__name__}/{self.gid}"


class AbstractBrokerConn(ABC):
    def next(self, broker_state: BrokerState) -> Optional[Time]:
        """Advance the time by one tick. Updates prices, executes orders, etc"""

    def fill_order(self, order: Order, broker_state: BrokerState) -> None:
        """Add an order to the queue"""

    def get_default_numeraire(self) -> Optional[str]:
        """Returns the default numeraire"""


class NullBrokerConn(AbstractBrokerConn):
    """A minimal broker connection class that doesn't really do anything, but it's useful for testing"""

    def __init__(self):
        self.default_numeraire = 'EUR'

    def next(self, broker_state: BrokerState) -> Optional[Time]:
        self.default_numeraire = broker_state.default_numeraire
        broker_state.now = datetime.datetime.utcnow()
        broker_state.time_index = broker_state.time_index + 1 if broker_state.time_index else 0
        return broker_state.now

    def fill_order(self, order: Order, broker_state: BrokerState) -> None:
        return None

    def get_default_numeraire(self) -> Optional[str]:
        return self.default_numeraire


class Broker:
    """Wrapper class defining the broker interface

    Trading strategies talk to this class.

    TODOs:
    * Add property caching
    """

    _broker_state: BrokerState

    def __init__(
        self,
        broker_conn: AbstractBrokerConn,
        initial_orders: List[Order],
        broker_state: Optional[BrokerState] = None,
    ):
        self._broker_conn = broker_conn
        self._no_postponed_orders_threshold = 8
        self._broker_state = (
            broker_state if broker_state else BrokerState(broker_conn.get_default_numeraire())
        )
        self._broker_state.active_orders.extend(initial_orders)
        self.next()  # initialize the broker_state and execute initial orders

    def next(self) -> Optional[Time]:
        """Note that this class is not an iterator because independent iterations are not supported"""
        result = self._broker_conn.next(self._broker_state)
        self._broker_state.check()
        if result is None:
            return None

        self._broker_state.recent_prices.update(self._broker_state.current_prices)
        # This executes if orders start piling up in the queue and report the queue status
        if len(self._broker_state.active_orders) > self._no_postponed_orders_threshold:
            classes = [type(o).__name__ for o in self._broker_state.active_orders]
            summary = ' '.join(f'{c}:{i}' for c, i in collections.Counter(classes).items())
            logger.warning(
                f'More than {self._no_postponed_orders_threshold} orders postponed: {summary}'
            )
            self._no_postponed_orders_threshold *= 2
        return self._broker_state.now

    def fill_order(self, order: Order) -> None:
        assert self._broker_state.default_numeraire, 'Default numeraire not set'
        assert self._broker_state.now, 'Now is not set'

        logger.info(
            f'T{self._broker_state.time_index} {self._broker_state.now}: Fill: {str(order)}'
        )
        order.set_status(OrderStatus.ACTIVE, self.get_time())
        self._broker_conn.fill_order(order, self._broker_state)

    def get_value_portfolio(self, num0: str = '') -> Optional[float]:
        """Sum all recent account values"""
        if num0 == '':
            num0 = self.get_default_numeraire()
        account_values = self.get_value_all_accounts(num0)
        if any([value is None for value in account_values.values()]):
            return None
        return sum(account_values.values())

    def get_value_account(self, acc: str, num0: str = '') -> Optional[float]:
        """Calc recent value of an account"""
        if num0 == '':
            num0 = self.get_default_numeraire()
        if acc not in self.accounts:
            return None
        acc_value, acc_num = self.accounts[acc]
        if acc_value < 0.0:
            last_price = 1.0 / calc_path_price(self.get_recent_prices(), num0, acc_num)
        else:
            last_price = calc_path_price(self.get_recent_prices(), acc_num, num0)
        if last_price is None or not math.isfinite(last_price):
            return None
        return acc_value * last_price

    @property
    def recent_value_all_accounts(self):
        return self.get_value_all_accounts(self.get_default_numeraire())

    def get_value_all_accounts(self, num0: str = '') -> Dict[str, Optional[float]]:
        """Calc recent value for all accounts using recent prices"""
        if num0 == '':
            num0 = self.get_default_numeraire()
        values = {acc: self.get_value_account(acc, num0) for acc in self.accounts}
        return values

    @property
    def recent_weights_all_accounts(self) -> Dict[str, Optional[float]]:
        return self.get_weight_all_accounts()

    def get_weight_all_accounts(self) -> Dict[str, Optional[float]]:
        """Calc recent weights for all accounts"""
        position_values = self.get_value_all_accounts()
        portfolio_value = self.get_value_portfolio()
        if portfolio_value is None or portfolio_value < NEGLIGIBLE_POSITIVE_PORTFOLIO_NAV:
            return {key: None for key in position_values.keys()}
        return {
            key: None if value is None else value / portfolio_value
            for key, value in position_values.items()
        }

    def get_account_weight(self, account_name: str) -> Optional[float]:
        portfolio_value = self.get_value_portfolio()
        if portfolio_value is None or portfolio_value < NEGLIGIBLE_POSITIVE_PORTFOLIO_NAV:
            return None
        return self.get_value_account(account_name) / portfolio_value

    def get_active_orders(self) -> List[Order]:
        return list(self._broker_state.active_orders)

    def get_executed_orders(self) -> List[Order]:
        return list(self._broker_state.executed_orders)

    def get_current_price(self, num0: str, num1: str) -> Optional[float]:
        return get_price_from_dict(self._broker_state.current_prices, num0, num1)

    @property
    def recent_prices(self):
        return MappingProxyType(self._broker_state.recent_prices)

    def get_recent_prices(self) -> Mapping[Tuple[str, str], float]:
        return MappingProxyType(self._broker_state.recent_prices)

    def current_price_graph_is_full(
        self, cash_nums: Iterable[str], asset_nums: Iterable[str]
    ) -> bool:
        """Do we have current prices for all given numeraires available?"""
        return price_graph_is_full(self._broker_state.current_prices, cash_nums, asset_nums)

    def recent_price_graph_is_full(
        self, cash_nums: Iterable[str], asset_nums: Iterable[str]
    ) -> bool:
        """Do we have recent prices for all given numeraires available?"""
        return price_graph_is_full(self._broker_state.recent_prices, cash_nums, asset_nums)

    def get_time(self) -> Optional[Time]:
        return self._broker_state.now

    def get_time_index(self) -> Optional[int]:
        return self._broker_state.time_index

    @property
    def accounts(self) -> Mapping[str, Amount]:
        return MappingProxyType(self._broker_state.accounts)

    def get_accounts(self) -> Mapping[str, Amount]:
        return MappingProxyType(self._broker_state.accounts)

    @property
    def variables(self) -> Mapping[str, Union[float, str]]:
        return MappingProxyType(self._broker_state.variables)

    def get_variables(self) -> Mapping[str, Union[float, str]]:
        return MappingProxyType(self._broker_state.variables)

    def get_default_numeraire(self) -> Optional[str]:
        return self._broker_state.default_numeraire

    def get_recent_trade_edges(self) -> KeysView[Tuple[str, str]]:
        """Returns numeraire pairs tradeable now or in the past"""
        return self._broker_state.recent_prices.keys()

    def get_current_trade_edges(self) -> KeysView[Tuple[str, str]]:
        """Returns numeraire pairs tradeable now"""
        return self._broker_state.current_prices.keys()
