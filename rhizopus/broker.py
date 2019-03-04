import collections
import logging
import datetime
from collections import deque
from types import MappingProxyType
from typing import Mapping, Dict, Tuple, Optional, Union, KeysView
from . import Amount, calc_path_price, get_price_from_dict


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

    This class is passed to concrete brokers and each one of those
    maps their internal state to the fields of this class.
    """
    variables: Dict[str, Union[float, str]]
    accounts: Dict[str, Amount]
    current_prices: Dict[Tuple[str, str], float]
    recent_prices: Dict[Tuple[str, str], float]
    now: Optional[datetime.datetime]
    time_index: int

    def __init__(self, default_numeraire: str = None, accounts: dict = None, variables: dict = None):
        self.accounts = dict(accounts) if accounts is not None else {}
        self.variables = dict(variables) if variables is not None else {}
        self.current_prices = {}
        self.recent_prices = {}
        self.default_numeraire = default_numeraire
        self.now = None
        self.time_index = 0
        self.active_orders = deque(maxlen=1000)
        self.executed_orders = deque(maxlen=1000)


class Broker:
    """Wrapper class defining the broker interface"""
    __broker_state: BrokerState

    def __init__(self, broker, initial_orders: list):
        self.__broker = broker
        self.__no_postponed_orders_threshold = 8
        self.__logger = logging.getLogger(__name__)
        self.__broker_state = BrokerState()
        self.__broker_state.active_orders.extend(initial_orders)
        self.next()  # initialize the broker_state and execute initial orders

    def next(self):
        """Note that this class is not an iterator because independent iterations
        are not possible"""
        result = self.__broker.next(self.__broker_state)
        if result is None:
            return None
        self.__broker_state.recent_prices.update(self.__broker_state.current_prices)
        if len(self.__broker_state.active_orders) > self.__no_postponed_orders_threshold:
            classes = [type(o).__name__ for o in self.__broker_state.active_orders]
            summary = ' '.join(f'{c}:{i}' for c, i in collections.Counter(classes).items())
            self.__logger.warning(f'More than {self.__no_postponed_orders_threshold} orders postponed: {summary}')
            self.__no_postponed_orders_threshold *= 2
        return self.__broker_state.now

    def fill_order(self, order):
        assert self.__broker_state.default_numeraire is not None, 'Default numeraire not set'
        assert len(self.__broker_state.default_numeraire) != 0, 'Default numeraire not set'
        assert self.__broker_state is not None, 'Now is not set'

        self.__logger.info(
            f'T{self.__broker_state.time_index} {self.__broker_state.now}: Fill: {str(order)}')
        self.__broker.fill_order(order, self.__broker_state)

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
        if num0 is None:
            return None
        accounts = self.get_accounts()
        if acc not in accounts.keys():
            return None
        last_price = calc_path_price(self.get_recent_prices(), accounts[acc][1], num0)
        if last_price is None:
            return None
        return accounts[acc][0] * last_price

    def get_value_all_accounts(self, num0: str = '') -> Dict[str, Optional[float]]:
        """Calc recent value for all accounts using recent prices """
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
        return {key: None if value is None else value/portfolio_value for key, value in position_values.items()}

    def get_active_orders(self):
        return self.__broker_state.active_orders

    def get_executed_orders(self):
        return self.__broker_state.executed_orders

    def get_current_price(self, num0: str, num1: str) -> Optional[float]:
        return get_price_from_dict(self.__broker_state.current_prices, num0, num1)

    def get_recent_prices(self):
        return MappingProxyType(self.__broker_state.recent_prices)

    def get_time(self) -> Optional[datetime.datetime]:
        return self.__broker_state.now

    def get_time_index(self) -> Optional[int]:
        return self.__broker_state.time_index

    def get_accounts(self) -> Mapping[str, Amount]:
        return MappingProxyType(self.__broker_state.accounts)

    def get_variables(self) -> Mapping[str, Union[float, str]]:
        return MappingProxyType(self.__broker_state.variables)

    def get_default_numeraire(self) -> Optional[str]:
        return self.__broker_state.default_numeraire

    def get_recent_trade_edges(self) -> KeysView[Tuple[str, str]]:
        """ Returns numeraire pairs tradeable now or in the past """
        return self.__broker_state.recent_prices.keys()

    def get_current_trade_edges(self) -> KeysView[Tuple[str, str]]:
        """ Returns numeraire pairs tradeable now or in the past """
        return self.__broker_state.current_prices.keys()
