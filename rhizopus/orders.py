from typing import Dict, Union

from rhizopus.broker import BrokerError, BrokerState, Order, OrderStatus
from rhizopus.price_graph import get_price_from_dict
from rhizopus.primitives import (
    Amount,
    checked_amount,
    checked_str_id,
    checked_value,
)


class ObserveInstrumentOrder(Order):
    def __init__(self, instrument: str):
        super().__init__()
        self.instrument = checked_str_id(instrument)

    def execute(self, broker_state: BrokerState) -> OrderStatus:
        raise NotImplementedError

    def __str__(self):
        return f'{self.__class__.__name__}/{self.gid}: {self.instrument}'


class CreateAccountOrder(Order):
    def __init__(self, account_name: str, amount: Amount, gid: int = 0):
        super().__init__(gid)
        self.account_name = checked_str_id(account_name)
        self.amount = checked_amount(amount)

    def execute(self, broker_state: BrokerState) -> OrderStatus:
        if self.account_name in broker_state.accounts.keys():
            return self.set_status(
                OrderStatus.REJECTED,
                broker_state.now,
                f'Account {self.account_name} already exists',
            )
        broker_state.accounts[self.account_name] = self.amount
        return OrderStatus.EXECUTED

    def __repr__(self):
        return f'{self.__class__.__name__}("{self.account_name}", ({self.amount[0]}, "{self.amount[1]}"))'

    def __str__(self):
        return f'{self.__class__.__name__}/{self.gid}: {self.account_name}, ({self.amount[0]}, {self.amount[1]})'


class DeleteAccountOrder(Order):
    def __init__(self, account_name: str, gid: int = 0):
        super().__init__(gid)
        self.account_name = checked_str_id(account_name)

    def execute(self, broker_state: BrokerState) -> OrderStatus:
        """Order will wait until the target account is defunded and delete it"""
        if self.account_name not in broker_state.accounts.keys():
            return self.set_status(
                OrderStatus.REJECTED,
                broker_state.now,
                f'{self.__class__.__name__}: Account {self.account_name} not found',
            )
        if abs(broker_state.accounts[self.account_name][0]) > 1e-12:
            return self.set_status(OrderStatus.ACTIVE, broker_state.now)
        del broker_state.accounts[self.account_name]
        return self.set_status(OrderStatus.EXECUTED, broker_state.now)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and vars(self) == vars(other)

    def __repr__(self):
        return f'{self.__class__.__name__}("{self.account_name}")'

    def __str__(self):
        return f'{self.__class__.__name__}/{self.gid}: {self.account_name}'


class TransferAllOrder(Order):
    def __init__(self, acc0: str, acc1: str, persistent: bool = False, gid: int = 0):
        """Transfer all wealth from acc0 to acc1"""
        super().__init__(gid)
        self.acc0 = checked_str_id(acc0)
        self.acc1 = checked_str_id(acc1)
        if self.acc0 == self.acc1:
            raise ValueError(f'Source and destination accounts must be different: {self.acc0}')
        self.persistent = persistent

    def execute(self, broker_state: BrokerState) -> OrderStatus:
        if (
            self.acc0 not in broker_state.accounts.keys()
            or self.acc1 not in broker_state.accounts.keys()
        ):
            return (
                OrderStatus.ACTIVE
                if self.persistent
                else self.set_status(OrderStatus.EXECUTED, broker_state.now)
            )
        amount = broker_state.accounts[self.acc0]
        if abs(amount[0]) < 1e-12:
            return (
                OrderStatus.ACTIVE
                if self.persistent
                else self.set_status(OrderStatus.EXECUTED, broker_state.now)
            )

        order = ForwardTransferOrder(self.acc0, self.acc1, amount, gid=self.gid)
        order.execute(broker_state)
        return (
            OrderStatus.ACTIVE
            if self.persistent
            else self.set_status(OrderStatus.EXECUTED, broker_state.now)
        )

    def __eq__(self, other):
        return (
            isinstance(other, self.__class__)
            and self.acc0 == other.acc0
            and self.acc1 == other.acc1
        )

    def __repr__(self):
        return f"{self.__class__.__name__}({self.acc0}, {self.acc1})"

    def __str__(self):
        return f'{self.__class__.__name__}/{self.gid}: {self.acc0} {self.acc1}'


class BackwardTransferOrder(Order):
    def __init__(self, acc0: str, acc1: str, amount: Amount, gid: int = 0):
        """Transfer wealth from acc0 to acc1 and target the specified amount change in acc1"""
        super().__init__(gid)
        self.acc0 = checked_str_id(acc0)
        self.acc1 = checked_str_id(acc1)
        if self.acc0 == self.acc1:
            raise ValueError(f'Source and destination accounts must be different: {self.acc0}')
        self.amount = checked_amount(amount)

    def execute(self, broker_state: BrokerState) -> OrderStatus:
        acc0 = self.acc0
        acc1 = self.acc1
        if acc0 not in broker_state.accounts.keys() or acc1 not in broker_state.accounts.keys():
            return self.set_status(
                OrderStatus.REJECTED,
                broker_state.now,
                f'Unable to transfer from or to a non-existent account: "{acc0}" "{acc1}"',
            )
        value0, num0 = broker_state.accounts[acc0]
        value1, num1 = broker_state.accounts[acc1]
        order_value, order_num = self.amount

        if order_value >= 0.0:
            price_a = get_price_from_dict(broker_state.current_prices, num0, num1)
            price_b = get_price_from_dict(broker_state.current_prices, num1, order_num)
        else:
            price_a = get_price_from_dict(broker_state.current_prices, num1, num0)
            price_b = get_price_from_dict(broker_state.current_prices, order_num, num1)
        if price_a is None or price_b is None:
            return OrderStatus.ACTIVE
        if price_a < 0.0 or price_b < 0.0:
            raise BrokerError(
                f'Negative prices for {num0} {num1} {order_num} detected: {price_a} {price_b}'
            )
        if order_value >= 0.0:
            new_acc0 = (value0 - order_value / (price_a * price_b), num0)
            new_acc1 = (value1 + order_value / price_b, num1)
        else:
            new_acc0 = (value0 - order_value * price_a * price_b, num0)
            new_acc1 = (value1 + order_value * price_b, num1)
        broker_state.accounts[acc0] = new_acc0
        broker_state.accounts[acc1] = new_acc1
        return self.set_status(OrderStatus.EXECUTED, broker_state.now)

    def __eq__(self, other):
        return transfer_order_comparator(self, other)

    def __repr__(self):
        return f'{self.__class__.__name__}({self.acc0}, {self.acc1}, {self.amount})'

    def __str__(self):
        return f'{self.__class__.__name__}/{self.gid}: {self.acc0} ({self.amount[0]} {self.amount[1]}) {self.acc1}'


class ForwardTransferOrder(Order):
    def __init__(self, acc0: str, acc1: str, amount: Amount, gid: int = 0):
        """Transfer wealth from acc0 to acc1 and target the specified amount change in acc0"""
        super().__init__(gid)
        self.acc0 = checked_str_id(acc0)
        self.acc1 = checked_str_id(acc1)
        if self.acc0 == self.acc1:
            raise ValueError(f'Source and destination accounts must be different: {self.acc0}')
        self.amount = checked_amount(amount)

    def execute(self, broker_state: BrokerState) -> OrderStatus:
        acc0 = self.acc0
        acc1 = self.acc1
        if acc0 not in broker_state.accounts.keys() or acc1 not in broker_state.accounts.keys():
            return self.set_status(
                OrderStatus.REJECTED,
                broker_state.now,
                f'Unable to transfer from or to a non-existent account: "{acc0}" "{acc1}"',
            )
        value0, num0 = broker_state.accounts[acc0]
        value1, num1 = broker_state.accounts[acc1]
        order_value, order_num = self.amount

        if order_value >= 0.0:
            price_a = get_price_from_dict(broker_state.current_prices, num0, order_num)
            price_b = get_price_from_dict(broker_state.current_prices, num0, num1)
        else:
            price_a = get_price_from_dict(broker_state.current_prices, order_num, num0)
            price_b = get_price_from_dict(broker_state.current_prices, num1, num0)
        if price_a is None or price_b is None:
            return OrderStatus.ACTIVE
        if price_a < 0.0 or price_b < 0.0:
            raise BrokerError(
                f'Negative prices for {num0} {num1} {order_num} detected: {price_a} {price_b}'
            )
        if order_value >= 0.0:
            # Send the wealth needed to buy 'amount' from acc0 to acc1
            new_acc0 = (value0 - order_value / price_a, num0)
            new_acc1 = (value1 + order_value * price_b / price_a, num1)
        else:
            # The amount is sold and transferred to acc0. This is financed using acc1.
            new_acc0 = (value0 - order_value * price_a, num0)
            new_acc1 = (value1 + order_value * price_a / price_b, num1)
        broker_state.accounts[acc0] = new_acc0
        broker_state.accounts[acc1] = new_acc1
        return self.set_status(OrderStatus.EXECUTED, broker_state.now)

    def __eq__(self, other):
        return transfer_order_comparator(self, other)

    def __repr__(self):
        return f"{self.__class__.__name__}({self.acc0}, {self.acc1}, {self.amount})"

    def __str__(self):
        return f'{self.__class__.__name__}/{self.gid}: {self.acc0} ({self.amount[0]} {self.amount[1]}) {self.acc1}'


def transfer_order_comparator(o1, o2):
    if not isinstance(o1, o2.__class__):
        return False
    if (
        o1.acc0 == o2.acc0
        and o1.acc1 == o2.acc1
        and o1.amount[1] == o2.amount[1]
        and abs(o1.amount[0] - o2.amount[0]) < 1e-12
    ):
        return True
    return False


class AddToVariableOrder(Order):
    def __init__(self, variable_name: str, value: float, gid: int = 0):
        super().__init__(gid)
        self.variable_name = checked_str_id(variable_name)
        value = checked_value(self.variable_name, value)
        self.value = value

    def execute(self, broker_state: BrokerState) -> OrderStatus:
        if self.variable_name in broker_state.variables.keys():
            broker_state.variables[self.variable_name] += self.value
        else:
            broker_state.variables[self.variable_name] = self.value
        return self.set_status(OrderStatus.EXECUTED, broker_state.now)

    def __str__(self):
        if self.value < 0:
            return (
                f"{self.__class__.__name__}/{self.gid}: {self.variable_name} -= {abs(self.value)}"
            )
        return f"{self.__class__.__name__}/{self.gid}: {self.variable_name} += {self.value}"


class UpdateVariablesOrder(Order):
    def __init__(self, vars_update: Dict[str, Union[float, str]], gid: int = 0):
        super().__init__(gid)
        # TODO check vars_update keys and values more precisely
        assert len(vars_update) > 0
        self.vars_update = vars_update

    def execute(self, broker_state: BrokerState) -> OrderStatus:
        broker_state.variables.update(self.vars_update)
        return self.set_status(OrderStatus.EXECUTED, broker_state.now)

    def __str__(self):
        return f"{self.__class__.__name__}/{self.gid}: " + ' '.join(
            [str(k) + '=' + str(v) for k, v in self.vars_update.items()]
        )


class AddToAccountBalanceOrder(Order):
    def __init__(self, account_name: str, value: float, gid: int = 0):
        super().__init__(gid)
        self.account_name = checked_str_id(account_name)
        value = checked_value(self.account_name, value)
        self.value = value

    def execute(self, broker_state: BrokerState) -> OrderStatus:
        if self.account_name not in broker_state.accounts.keys():
            return self.set_status(
                OrderStatus.REJECTED, broker_state.now, f'Account {self.account_name} not found.'
            )
        old_value, num = broker_state.accounts[self.account_name]
        broker_state.accounts[self.account_name] = (old_value + self.value, num)
        return self.set_status(OrderStatus.EXECUTED, broker_state.now)

    def __str__(self):
        if self.value < 0:
            return f"{self.__class__.__name__}/{self.gid}: {self.account_name} -= {abs(self.value)}"
        return f"{self.__class__.__name__}/{self.gid}: {self.account_name} += {self.value}"


class CfdOpenOrder(Order):
    def execute(self, broker_state: BrokerState) -> OrderStatus:
        raise NotImplementedError

    def __init__(self, num0: str, num1: str, units: float, gid: int = 0):
        super().__init__(gid)
        self.num0 = checked_str_id(num0)
        self.num1 = checked_str_id(num1)
        if self.num0 == self.num1:
            raise ValueError(f'Please specify two different numeraires: {self.num0}')
        self.units = checked_value(f'{num0} {num1}', units)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        if (
            self.num0 == other.num0
            and self.num1 == other.num1
            and abs(self.units - other.units) < 1e-12
        ):
            return True
        if (
            self.num0 == other.num1
            and self.num1 == other.num0
            and abs(self.units + other.units) < 1e-12
        ):
            return True
        return False

    def __repr__(self):
        return f'{self.__class__.__name__}("{self.num0}", "{self.num1}", {self.units})'

    def __str__(self):
        return f'{self.__class__.__name__}/{self.gid}: {self.num0}_{self.num1}: {self.units}'


class CfdCloseOrder(Order):
    def execute(self, broker_state: BrokerState) -> OrderStatus:
        raise NotImplementedError

    def __init__(self, acc0: str, acc1: str, gid: int = 0):
        super().__init__(gid)
        self.acc0 = checked_str_id(acc0)
        self.acc1 = checked_str_id(acc1)
        if self.acc0 == self.acc1:
            raise ValueError(f'Source and destination accounts must be different: {self.acc0}')

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        if self.acc0 == other.acc0 and self.acc1 == other.acc1:
            return True
        if self.acc0 == other.acc1 and self.acc1 == other.acc0:
            return True
        return False

    def __repr__(self):
        return f'{self.__class__.__name__}("{self.acc0}", "{self.acc1}")'

    def __str__(self):
        return f'{self.__class__.__name__}/{self.gid}: {self.acc0}, {self.acc1}'


class CfdReduceOrder(Order):
    def execute(self, broker_state: BrokerState) -> OrderStatus:
        raise NotImplementedError

    def __init__(self, acc0: str, acc1: str, units: float, gid: int = 0):
        """Reduce a Cfd trade by opening an opposite trade and merging both together

        The meaning of the parameters corresponds to that of the CfdOpenOrder
        """
        super().__init__(gid)
        self.acc0 = checked_str_id(acc0)
        self.acc1 = checked_str_id(acc1)
        if self.acc0 == self.acc1:
            raise ValueError(f'Source and destination accounts must be different: {self.acc0}')

        units = checked_value(f'{acc0} {acc1}', units)
        self.units0 = units

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        if (
            self.acc0 == other.acc0
            and self.acc1 == other.acc1
            and abs(self.units0 - other.units0) < 1e-12
        ):
            return True
        return False

    def __repr__(self):
        return f'{self.__class__.__name__}("{self.acc0}", "{self.acc1}", {self.units0})'

    def __str__(self):
        return f'{self.__class__.__name__}/{self.gid}: {self.acc0}, {self.acc1}, {self.units0}'
