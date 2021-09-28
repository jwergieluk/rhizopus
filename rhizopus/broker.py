import collections
import logging
from abc import ABC
from collections import deque
from enum import Enum, auto
from types import MappingProxyType
from typing import Dict, KeysView, List, Mapping, Optional, Tuple, Union

from rhizopus.primitives import Time, Amount, raise_for_time
from rhizopus.price_graph import calc_path_price, get_price_from_dict

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

    def __init__(
        self,
        default_numeraire: str,
        accounts: Optional[Dict[str, Amount]] = None,
        variables: Optional[Dict[str, Union[float, str]]] = None,
    ):
        assert default_numeraire, "Numeraire has to be a non-empty string"
        self.accounts = dict(accounts) if accounts else {}
        self.variables = dict(variables) if variables else {}
        assert all(self.accounts.keys()), "Account names must be non-empty strings"
        assert all(self.variables.keys()), "Variable names must be non-empty strings"
        self.current_prices = {}
        self.recent_prices = {}
        self.default_numeraire = default_numeraire
        self.now = None
        self.time_index = 0
        self.active_orders = deque(maxlen=50000)
        self.executed_orders = deque(maxlen=100000)

    def check(self):
        """Self-check"""
        if not (type(self.default_numeraire) == str and self.default_numeraire):
            raise BrokerStateError(f'Wrong default numeraire: {self.default_numeraire}')
        if not (type(self.time_index) == int and self.time_index >= 0):
            raise BrokerStateError(f'Wrong time index: {self.time_index}')

        raise_for_time(self.now)


class OrderStatus(Enum):
    NEW = auto()
    PROCESSING = auto()
    ACCEPTED = auto()
    REJECTED = auto()


class Order:
    """Represents an order executed by a Broker instance

    An order is not allowed to have variable state which must be stored in BrokerState.
    The execute() method is invoked only by the BrokerSimulator.
    """

    def __init__(self, gid: int = 0):
        self.age = 0
        self.status = OrderStatus.NEW
        self.transaction_id = None
        self.gid = gid

    def execute(self, broker_state: BrokerState):
        """Order execution in the simulation environment"""
        raise NotImplementedError

    def __repr__(self):
        return f"{self.__class__.__name__}()"

    def __str__(self):
        return f"{self.__class__.__name__}/{self.gid}"


class AbstractBrokerConn(ABC):
    def next(self, broker_state: BrokerState):
        """Advance the time by one tick. Updates prices, executes orders, etc"""

    def fill_order(self, order: Order, broker_state: BrokerState):
        """Add an order to the queue"""

    def get_default_numeraire(self) -> Optional[str]:
        """Returns the default numeraire"""


class Broker:
    """Wrapper class defining the broker interface

    Trading strategies talk to this class.
    """

    _broker_state: BrokerState

    def __init__(self, broker_conn: AbstractBrokerConn, initial_orders: List[Order]):
        self._broker_conn = broker_conn
        self._no_postponed_orders_threshold = 8
        self._broker_state = BrokerState(broker_conn.get_default_numeraire())
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

    def fill_order(self, order: Order):
        assert self._broker_state.default_numeraire, 'Default numeraire not set'
        assert self._broker_state.now, 'Now is not set'

        logger.info(
            f'T{self._broker_state.time_index} {self._broker_state.now}: Fill: {str(order)}'
        )
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
        accounts = self.get_accounts()
        if acc not in accounts.keys():
            return None
        last_price = calc_path_price(self.get_recent_prices(), accounts[acc][1], num0)
        if last_price is None:
            return None
        return accounts[acc][0] * last_price

    def get_value_all_accounts(self, num0: str = '') -> Dict[str, Optional[float]]:
        """Calc recent value for all accounts using recent prices"""
        if num0 == '':
            num0 = self.get_default_numeraire()
        values = {}
        accounts = self.get_accounts()
        for name, amount in accounts.items():
            last_price = calc_path_price(self.get_recent_prices(), amount[1], num0)
            if last_price is None:
                values[name] = None
            else:
                values[name] = amount[0] * last_price
        return values

    def get_weight_all_accounts(self) -> Dict[str, Optional[float]]:
        """Calc recent weights for all accounts"""
        position_values = self.get_value_all_accounts()
        portfolio_value = self.get_value_portfolio()
        if portfolio_value is not None and abs(portfolio_value) < 1e-8:
            portfolio_value = None
        if portfolio_value is None:
            return {key: None for key in position_values.keys()}
        return {
            key: None if value is None else value / portfolio_value
            for key, value in position_values.items()
        }

    def get_active_orders(self):
        return self._broker_state.active_orders

    def get_executed_orders(self):
        return self._broker_state.executed_orders

    def get_current_price(self, num0: str, num1: str) -> Optional[float]:
        return get_price_from_dict(self._broker_state.current_prices, num0, num1)

    def get_recent_prices(self):
        return MappingProxyType(self._broker_state.recent_prices)

    def get_time(self) -> Optional[Time]:
        return self._broker_state.now

    def get_time_index(self) -> Optional[int]:
        return self._broker_state.time_index

    def get_accounts(self) -> Mapping[str, Amount]:
        return MappingProxyType(self._broker_state.accounts)

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
