import pytest
from rhizopus.broker import BrokerState, BrokerError
from rhizopus.orders import (
    CfdOpenOrder,
    CfdCloseOrder,
    CfdReduceOrder,
    CreateAccountOrder,
    AddToVariableOrder,
    AddToAccountBalanceOrder,
)


def test_cfd_open_order_eq1():
    o1 = CfdOpenOrder('EUR', 'USD', 1.0)
    o2 = CfdOpenOrder('USD', 'EUR', -1.0)
    assert o1 == o2


def test_cfd_open_order__eq2():
    o1 = CfdOpenOrder('EUR', 'USD', 1.0)
    o2 = CfdOpenOrder('EUR', 'USD', -1.1)
    o3 = CfdOpenOrder('EUR', 'XAU', -1.0)
    assert o1 != o2
    assert o1 != o3
    assert o2 != o3


def test_cfd_open_order__repr():
    o1 = CfdOpenOrder('EUR', 'USD', 1.0)
    o2 = eval(repr(o1))
    assert o1 == o2


def test_cfd_close_order_eq1():
    o1 = CfdCloseOrder('CFD_1_EUR', 'CFD_1_USD')
    o2 = CfdCloseOrder('CFD_1_USD', 'CFD_1_EUR')
    o3 = CfdCloseOrder('CFD_3_USD', 'CFD_3_EUR')
    assert o1 == o2
    assert o1 == o1
    assert o1 != o3


def test_cfd_close_order_repr():
    o1 = CfdCloseOrder('CFD_1_EUR', 'CFD_1_USD')
    o2 = eval(repr(o1))
    assert o1 == o2


def test_cfd_reduced_order_eq1():
    o1 = CfdReduceOrder('CFD_1_EUR', 'CFD_1_USD', 1.0)
    o2 = CfdReduceOrder('CFD_1_USD', 'CFD_1_EUR', 1.0)
    o3 = CfdReduceOrder('CFD_1_EUR', 'CFD_1_USD', -1.0)
    o4 = CfdReduceOrder('CFD_1_USD', 'CFD_1_EUR', -1.0)
    assert o1 == o1
    assert o1 != o2
    assert o1 != o3
    assert o1 != o4


def test_cfd_reduced_order_repr():
    o1 = CfdReduceOrder('CFD_1_EUR', 'CFD_1_USD', 1.0)
    o2 = eval(repr(o1))
    assert o1 == o2


def test_create_account_order1():
    broker_state = BrokerState(default_numeraire='EUR', accounts={'EUR': (0, 'EUR')}, variables={})

    order = CreateAccountOrder('USD', (0.0, 'USD'))
    order.execute(broker_state)
    assert 'USD' in broker_state.accounts.keys()
    assert broker_state.accounts['USD'], (0.0, 'USD')


def test_create_account_order2():
    broker_state = BrokerState(default_numeraire='EUR', accounts={'EUR': (0, 'EUR')}, variables={})
    order = CreateAccountOrder('EUR', (0.0, 'USD'))

    with pytest.raises(BrokerError):
        order.execute(broker_state)


def test_add_to_variable1():
    broker_state = BrokerState(default_numeraire='EUR', accounts={'EUR': (0, 'EUR')}, variables={})
    AddToVariableOrder('A', 5.0).execute(broker_state)

    assert ['A'] == list(broker_state.variables.keys())
    assert broker_state.variables['A'] == 5.0
    AddToVariableOrder('A', -5.0).execute(broker_state)

    assert ['A'] == list(broker_state.variables.keys())
    assert broker_state.variables['A'] == 0.0


def test_add_to_account_value1():
    broker_state = BrokerState(default_numeraire='EUR', accounts={'EUR': (0, 'EUR')}, variables={})
    AddToAccountBalanceOrder('EUR', 10.0).execute(broker_state)

    assert ['EUR'] == list(broker_state.accounts.keys())
    assert broker_state.accounts['EUR'][0] == 10.0
    assert broker_state.accounts['EUR'][1] == 'EUR'
