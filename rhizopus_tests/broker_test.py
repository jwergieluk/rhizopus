import random
import pytest
from rhizopus.broker import BrokerState, BrokerStateError, NullBrokerConn, Broker


@pytest.fixture()
def sample_broker_state():
    def _sample_broker_state() -> BrokerState:
        numeraires = [f'NM{i}' for i in range(7)]
        accounts = {f'{n}_ACC': (random.gammavariate(50.0, 2.0), n) for n in numeraires}

        broker_state = BrokerState('STU', accounts, {'e': 2.72})
        return broker_state

    return _sample_broker_state


@pytest.mark.parametrize('num', ['', None, ('num', '0')])
def test_wrong_broker_state_default_numeraire(sample_broker_state, num):
    broker_state: BrokerState = sample_broker_state()
    broker_state.default_numeraire = num
    with pytest.raises(BrokerStateError):
        broker_state.check()


@pytest.mark.parametrize('time_index', [-1, 0.0])
def test_wrong_broker_state_time_index(sample_broker_state, time_index):
    broker_state: BrokerState = sample_broker_state()
    broker_state.time_index = time_index
    with pytest.raises(BrokerStateError):
        broker_state.check()


def test_account_value_with_price_spread_short_position():
    """Test whether bid-ask spreads are correctly used when valuating short positions"""

    cash_num = 'EUR'
    capital = 0.0
    accounts = {cash_num: (capital, cash_num), 'NUM_LONG': (1.0, 'NUM'), 'NUM_SHORT': (-1.0, 'NUM')}
    broker_state = BrokerState(cash_num, accounts)

    prices = {
        ('NUM', cash_num): 1.0,
        (cash_num, 'NUM'): 0.5,
    }
    broker_state.current_prices = prices
    broker_state.recent_prices = dict(broker_state.current_prices)
    broker = Broker(NullBrokerConn(), [], broker_state)

    value_long = broker.get_value_account('NUM_LONG')
    value_short = broker.get_value_account('NUM_SHORT')

    assert value_long < abs(value_short)
    assert broker.get_value_portfolio() < 0.0
