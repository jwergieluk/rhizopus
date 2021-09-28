import random
import pytest
from rhizopus.broker import BrokerState, BrokerStateError


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
