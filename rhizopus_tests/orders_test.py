import unittest

from rhizopus.broker import BrokerState, BrokerError
from rhizopus.orders import (
    CfdOpenOrder,
    CfdCloseOrder,
    CfdReduceOrder,
    CreateAccountOrder,
    AddToVariableOrder,
    AddToAccountBalanceOrder,
)


class CfdOpenOrderTest(unittest.TestCase):
    def test_eq1(self):
        o1 = CfdOpenOrder('EUR', 'USD', 1.0)
        o2 = CfdOpenOrder('USD', 'EUR', -1.0)
        self.assertEqual(o1, o2)

    def test_eq2(self):
        o1 = CfdOpenOrder('EUR', 'USD', 1.0)
        o2 = CfdOpenOrder('EUR', 'USD', -1.1)
        o3 = CfdOpenOrder('EUR', 'XAU', -1.0)
        self.assertNotEqual(o1, o2)
        self.assertNotEqual(o1, o3)
        self.assertNotEqual(o2, o3)

    def test_repr(self):
        o1 = CfdOpenOrder('EUR', 'USD', 1.0)
        o2 = eval(repr(o1))
        self.assertEqual(o1, o2)


class CfdCloseOrderTest(unittest.TestCase):
    def test_eq1(self):
        o1 = CfdCloseOrder('CFD_1_EUR', 'CFD_1_USD')
        o2 = CfdCloseOrder('CFD_1_USD', 'CFD_1_EUR')
        o3 = CfdCloseOrder('CFD_3_USD', 'CFD_3_EUR')
        self.assertEqual(o1, o2)
        self.assertEqual(o1, o1)
        self.assertNotEqual(o1, o3)

    def test_repr(self):
        o1 = CfdCloseOrder('CFD_1_EUR', 'CFD_1_USD')
        o2 = eval(repr(o1))
        self.assertEqual(o1, o2)


class CfdReduceOrderTest(unittest.TestCase):
    def test_eq1(self):
        o1 = CfdReduceOrder('CFD_1_EUR', 'CFD_1_USD', 1.0)
        o2 = CfdReduceOrder('CFD_1_USD', 'CFD_1_EUR', 1.0)
        o3 = CfdReduceOrder('CFD_1_EUR', 'CFD_1_USD', -1.0)
        o4 = CfdReduceOrder('CFD_1_USD', 'CFD_1_EUR', -1.0)
        self.assertEqual(o1, o1)
        self.assertNotEqual(o1, o2)
        self.assertNotEqual(o1, o3)
        self.assertNotEqual(o1, o4)

    def test_repr(self):
        o1 = CfdReduceOrder('CFD_1_EUR', 'CFD_1_USD', 1.0)
        o2 = eval(repr(o1))
        self.assertEqual(o1, o2)


class CreateAccountOrderTest(unittest.TestCase):
    def test_create_account_order1(self):
        broker_state = BrokerState(
            default_numeraire='EUR', accounts={'EUR': (0, 'EUR')}, variables={}
        )

        order = CreateAccountOrder('USD', (0.0, 'USD'))
        order.execute(broker_state)
        self.assertTrue('USD' in broker_state.accounts.keys())
        self.assertEqual(broker_state.accounts['USD'], (0.0, 'USD'))

    def test_create_account_order2(self):
        broker_state = BrokerState(
            default_numeraire='EUR', accounts={'EUR': (0, 'EUR')}, variables={}
        )
        order = CreateAccountOrder('EUR', (0.0, 'USD'))

        with self.assertRaises(BrokerError):
            order.execute(broker_state)


class AddToVariableOrderTest(unittest.TestCase):
    def test_add_to_variable1(self):
        broker_state = BrokerState(
            default_numeraire='EUR', accounts={'EUR': (0, 'EUR')}, variables={}
        )
        AddToVariableOrder('A', 5.0).execute(broker_state)

        self.assertEqual(['A'], list(broker_state.variables.keys()))
        self.assertAlmostEqual(broker_state.variables['A'], 5.0)
        AddToVariableOrder('A', -5.0).execute(broker_state)

        self.assertEqual(['A'], list(broker_state.variables.keys()))
        self.assertAlmostEqual(broker_state.variables['A'], 0.0)


class AddToAccountOrderTest(unittest.TestCase):
    def test_add_to_account_value1(self):
        broker_state = BrokerState(
            default_numeraire='EUR', accounts={'EUR': (0, 'EUR')}, variables={}
        )
        AddToAccountBalanceOrder('EUR', 10.0).execute(broker_state)

        self.assertEqual(['EUR'], list(broker_state.accounts.keys()))
        self.assertAlmostEqual(broker_state.accounts['EUR'][0], 10.0)
        self.assertEqual(broker_state.accounts['EUR'][1], 'EUR')
