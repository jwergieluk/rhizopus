import collections
import datetime
import logging
import math
import random
import importlib
from abc import ABC
from collections import deque
from enum import Enum, auto
from types import MappingProxyType
from typing import Dict, KeysView, List, Mapping, Optional, Tuple, Union, Iterable, Any

from rhizopus.enums import enum_member_from_name
from rhizopus.price_graph import calc_path_price, get_price_from_dict, price_graph_is_full
from rhizopus.primitives import (
    Time,
    Amount,
    raise_for_time,
    raise_for_str_id,
    NEGLIGIBLE_POSITIVE_PORTFOLIO_NAV,
    checked_int_id,
    MIN_TIME,
    checked_amount,
    checked_real,
    amount_almost_eq,
    EPS_FINANCIAL,
    float_almost_equal,
    maybe_deserialize_time,
    maybe_serialize_time,
)

logger = logging.getLogger(__name__)


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

    MAX_NUM_ACTIVE_ORDERS = 50000
    MAX_NUM_EXECUTED_ORDERS = 100000
    MAX_NUM_REJECTED_ORDERS = 5000

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
        self.active_orders = deque(maxlen=self.MAX_NUM_ACTIVE_ORDERS)
        self.executed_orders = deque(maxlen=self.MAX_NUM_EXECUTED_ORDERS)
        self.rejected_orders = deque(maxlen=self.MAX_NUM_REJECTED_ORDERS)

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

        if random.uniform(0.0, 1.0) > 0.9:  # TODO Wrap in a with block
            other_self = self.from_json(self.to_json())
            assert self == other_self

    def to_json(self) -> Dict[str, Any]:
        """Serialize to JSON"""
        data = {
            'default_numeraire': self.default_numeraire,
            'now': '' if self.now is None else maybe_serialize_time(self.now),
            'time_index': self.time_index,
            'accounts': {acc: list(amount) for acc, amount in self.accounts.items()}
            if self.accounts
            else {},
            'variables': dict(self.variables) if self.variables else {},
            'current_prices': [
                [num0, num1, price] for (num0, num1), price in self.current_prices.items()
            ]
            if self.current_prices
            else {},
            'recent_prices': [
                [num0, num1, price] for (num0, num1), price in self.recent_prices.items()
            ]
            if self.recent_prices
            else {},
            'active_orders': [o.to_json() for o in self.active_orders],
            'executed_orders': [o.to_json() for o in self.executed_orders],
            'rejected_orders': [o.to_json() for o in self.rejected_orders],
        }
        return data

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> 'BrokerState':
        """Deserialize from JSON"""

        broker_state = BrokerState(data['default_numeraire'])
        broker_state.now = datetime.datetime.fromisoformat(data['now']) if data['now'] else None
        broker_state.time_index = checked_int_id(data['time_index'])
        broker_state.accounts = {
            acc: checked_amount(tuple(amount)) for acc, amount in data['accounts'].items()
        }
        broker_state.variables = data['variables']
        broker_state.current_prices = {
            (num0, num1): checked_real(num0 + num1, price, 0.0)
            for num0, num1, price in data['current_prices']
        }
        broker_state.recent_prices = {
            (num0, num1): checked_real(num0 + num1, price, 0.0)
            for num0, num1, price in data['recent_prices']
        }
        broker_state.active_orders = deque(
            (Order.from_json(o) for o in data['active_orders']), maxlen=cls.MAX_NUM_ACTIVE_ORDERS
        )
        broker_state.executed_orders = deque(
            (Order.from_json(o) for o in data['executed_orders']),
            maxlen=cls.MAX_NUM_EXECUTED_ORDERS,
        )
        broker_state.rejected_orders = deque(
            (Order.from_json(o) for o in data['rejected_orders']),
            maxlen=cls.MAX_NUM_REJECTED_ORDERS,
        )
        return broker_state

    def __eq__(self, other: 'BrokerState') -> bool:
        if not (
            self.default_numeraire == other.default_numeraire
            and self.now == other.now
            and self.time_index == other.time_index
            and set(self.accounts) == set(other.accounts)
            and set(self.current_prices) == set(other.current_prices)
            and set(self.recent_prices) == set(other.recent_prices)
            and set(self.variables) == set(other.variables)
        ):
            return False

        for acc in self.accounts:
            if not amount_almost_eq(self.accounts[acc], other.accounts[acc], EPS_FINANCIAL):
                return False
        for key in self.current_prices:
            if not float_almost_equal(
                self.current_prices[key], other.current_prices[key], EPS_FINANCIAL
            ):
                return False
        for key in self.recent_prices:
            if not float_almost_equal(
                self.recent_prices[key], other.recent_prices[key], EPS_FINANCIAL
            ):
                return False
        for var_name in self.variables:
            if isinstance(self.variables[var_name], float) and isinstance(
                other.variables[var_name], float
            ):
                if not float_almost_equal(
                    self.variables[var_name], other.variables[var_name], EPS_FINANCIAL
                ):
                    return False
            else:
                if self.variables[var_name] != other.variables[var_name]:
                    return False
        return True


class OrderStatus(Enum):
    ACTIVE = auto()
    EXECUTED = auto()
    REJECTED = auto()


ORDER_CLASSES_WITH_SERIALIZATION_SUPPORT = frozenset(
    (
        'ObserveInstrumentOrder',
        'CreateAccountOrder',
        'DeleteAccountOrder',
        'TransferAllOrder',
        'BackwardTransferOrder',
        'ForwardTransferOrder',
        'AddToVariableOrder',
        'AddToAccountBalanceOrder',
        'InterestOrder',
    )
)


class Order:
    """Represents an order executed by a Broker instance

    An order is not allowed to have variable state which must be stored in BrokerState.
    The execute() method is invoked only by the BrokerSimulator.
    """

    def __init__(
        self,
        age: int = 0,
        status: Union[OrderStatus, int] = OrderStatus.ACTIVE,
        status_time_stamp: Union[Time, str] = MIN_TIME,
        status_comment: str = '',
        transaction_id: int = 0,
        gid: int = 0,
        class_name: str = '',  # intentionally ignored
    ):
        self.age: int = checked_int_id(age)
        self.status: OrderStatus = (
            enum_member_from_name(OrderStatus, status) if isinstance(status, str) else status
        )
        # time stamp at which the current status was set
        self.status_time_stamp: Time = maybe_deserialize_time(status_time_stamp)
        self.status_comment: str = status_comment
        self.transaction_id: int = checked_int_id(transaction_id)  # TODO DELETEME
        self.gid: int = checked_int_id(gid)

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

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"

    def __str__(self) -> str:
        return f"{self.__class__.__name__}/{self.gid}"

    def __eq__(self, other: 'Order') -> bool:
        return (
            self.age == other.age
            and self.status == other.status
            and self.status_time_stamp == other.status_time_stamp
            and self.status_comment == other.status_comment
            and self.gid == other.gid
        )

    def to_json(self) -> Dict[str, Any]:
        data = {
            'age': self.age,
            'status': self.status.name,
            'status_time_stamp': maybe_serialize_time(self.status_time_stamp),
            'status_comment': self.status_comment,
            'transaction_id': self.transaction_id,
            'gid': self.gid,
            'class_name': self.__class__.__name__,
        }
        return data

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> 'Order':
        if 'class_name' not in data:
            raise ValueError(f'Missing class_name parameter: {data}')
        class_name = data['class_name']
        if class_name not in ORDER_CLASSES_WITH_SERIALIZATION_SUPPORT:
            raise ValueError(
                f'The order class "{class_name}" is not among orders supporting serialization: '
                f'{ORDER_CLASSES_WITH_SERIALIZATION_SUPPORT}. Order data: {data}'
            )
        module = importlib.import_module('rhizopus.orders')
        order_class: 'Order' = getattr(module, class_name)
        return order_class.from_json(data)


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
        silent: bool = False,
    ):
        self._broker_conn = broker_conn
        self._no_postponed_orders_threshold = 8
        self._broker_state = (
            broker_state if broker_state else BrokerState(broker_conn.get_default_numeraire())
        )
        self.silent = silent
        self._broker_state.active_orders.extend(initial_orders)
        if broker_state is None:
            self.next()  # initialize the broker_state and execute initial orders, if not initialized already

    def next(self) -> Optional[Time]:
        """Note that this class is not an iterator because independent iterations are not supported"""
        result = self._broker_conn.next(self._broker_state)
        self._broker_state.check()
        if result is None:
            return None

        self._broker_state.recent_prices.update(self._broker_state.current_prices)
        # This reports the queue status if orders start piling up in the queue
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

        if not self.silent:
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
        if (
            abs(acc_value) < EPS_FINANCIAL
        ):  # this returns the (vanishing) acc value even if no prices are available
            return 0.0
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

    @property
    def now(self) -> Optional[Time]:
        return self._broker_state.now

    def get_time(self) -> Optional[Time]:
        return self._broker_state.now

    @property
    def time_index(self) -> int:
        return self._broker_state.time_index

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

    def state_to_json(self) -> Dict[str, Any]:
        return self._broker_state.to_json()
