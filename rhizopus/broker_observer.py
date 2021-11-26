import logging
import datetime
from typing import (
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
    Any,
)

from rhizopus.broker import Broker
from rhizopus.primitives import Time, raise_for_key, maybe_serialize_time
from rhizopus.series_recorder import SeriesRecorder

# prefixes are namespaces
PORTFOLIO_PREFIX = 'portfolio'
ACCOUNT_PREFIX = 'account'
VARIABLE_PREFIX = 'var'
EVALUATOR_PREFIX = 'var'  # TODO change to 'eval'

logger = logging.getLogger(__name__)


class BrokerObserver:
    now: Optional[Time]

    def __init__(
        self,
        broker: Broker,
        rec_acc_weights: bool = True,
        rec_acc_navs: bool = True,
        rec_vars: bool = True,
    ):
        self.rec_vars = rec_vars
        self.rec_acc_navs = rec_acc_navs
        self.rec_acc_weights = rec_acc_weights
        self.broker = broker
        self.now = None

        self.recorder = SeriesRecorder()
        self.evaluators = dict()

    def add_evaluator(
        self, key: Union[str, Sequence[str]], func: Callable[[Broker], Optional[float]]
    ):
        raise_for_key(key)
        if key in self.evaluators:
            raise ValueError(f'Evaluator with the key "{key}" is already registered."')
        first_part = key[0] if isinstance(key, tuple) else key
        if first_part.startswith(EVALUATOR_PREFIX):
            raise ValueError(f'Evaluator key "{key}" must not start with "{EVALUATOR_PREFIX}"')
        key = (VARIABLE_PREFIX, *key) if isinstance(key, tuple) else (VARIABLE_PREFIX, key)
        self.evaluators[key] = func

    def save(
        self,
        key: Union[str, Sequence[str]],
        value: float,
        min_allowed: float = -1.0e24,
        max_allowed: float = 1e24,
        allow_nans: bool = False,
    ):
        if self.now is None:
            return
        self.recorder.save(self.now, key, value, min_allowed, max_allowed, allow_nans)

    def update(self):
        new_now = self.broker.get_time()
        if new_now is None:
            return
        if self.now is not None and new_now <= self.now:
            return
        self.now = new_now

        for key in self.broker.get_current_trade_edges():
            price = self.broker.get_current_price(*key)
            if price is not None:
                self.recorder.save(self.now, key, price)

        nav = self.broker.get_value_portfolio()
        if nav is not None:
            nav_key = (PORTFOLIO_PREFIX, 'nav')
            self.recorder.save(self.now, nav_key, nav, 0.0)
            nav_history = self.get_dict(nav_key)
            if len(nav_history) > 2:
                if abs(nav_history[min(nav_history.keys())]) > 1e-8:
                    total_return = (
                        nav / nav_history[min(nav_history.keys())] - 1.0
                    )  # ugly but cheap
                    self.recorder.save(self.now, (PORTFOLIO_PREFIX, 'total_return'), total_return)
                else:
                    logger.warning(
                        'NAV history starts with zero. Relative perf measures not available.'
                    )
            if self.rec_acc_weights:
                for account, weight in self.broker.get_weight_all_accounts().items():
                    if isinstance(weight, float):
                        self.recorder.save(self.now, (ACCOUNT_PREFIX, account, 'weight'), weight)

        if self.rec_acc_navs:
            for account, position_nav in self.broker.get_value_all_accounts().items():
                if isinstance(position_nav, float):
                    self.recorder.save(self.now, (ACCOUNT_PREFIX, account, 'nav'), position_nav)
        if self.rec_vars:
            for key, value in self.broker.get_variables().items():
                if isinstance(value, float):
                    self.recorder.save(self.now, (VARIABLE_PREFIX, key), float(value))

        for evaluator_key, evaluator in self.evaluators.items():
            value = evaluator(self.broker)
            if isinstance(value, float):
                self.recorder.save(self.now, evaluator_key, float(value))

    def get_dict(self, key: Union[str, Sequence[str]]) -> Optional[Mapping[Time, float]]:
        return self.recorder.get_dict(key)

    def get_list_of_pairs(
        self,
        key: Union[str, Sequence[str]],
        starting_with: Time = Time.min,
    ) -> Optional[Sequence[Tuple[Time, float]]]:
        return self.recorder.get_list_of_pairs(key, starting_with)

    def get_t_x(
        self,
        key: Union[str, Sequence[str]],
        starting_with: Time = Time.min,
    ) -> Tuple[Sequence[Time], Sequence[float]]:
        return self.recorder.get_t_x(key, starting_with)

    def get_history(self, key) -> Optional[Sequence[Tuple[Time, float]]]:
        return self.recorder.get_list_of_pairs(key)

    def get_history_portfolio_nav(self) -> Optional[Sequence[Tuple[Time, float]]]:
        return self.get_history(('portfolio', 'nav'))

    def get_history_portfolio_total_return(self) -> Optional[Sequence[Tuple[Time, float]]]:
        return self.get_history(('portfolio', 'total_return'))

    def keys(self):
        return self.recorder.keys()

    def get_recent_observations(self) -> Mapping[Union[str, Sequence[str]], float]:
        return self.recorder.get_recent_observations()

    def list_account_attributes(self):
        return [key for key in self.recorder.keys() if key[0] == 'account']

    def get_default_numeraire(self) -> Optional[str]:
        return self.broker.get_default_numeraire()

    def times(self) -> List[Time]:
        return self.recorder.times()

    def to_json(self) -> Dict[str, Any]:
        if self.evaluators:
            raise ValueError(
                f'An observer running custom evaluators can not be serialized: {list(self.evaluators)}'
            )
        return {
            'now': maybe_serialize_time(self.now) if self.now else '',
            'rec_acc_weights': self.rec_acc_weights,
            'rec_acc_navs': self.rec_acc_navs,
            'rec_vars': self.rec_vars,
            'time_series': self.recorder.to_json(),
        }

    @classmethod
    def from_json(cls, broker: Broker, data: Dict[str, Any]) -> 'BrokerObserver':
        observer = BrokerObserver(
            broker, data['rec_acc_weights'], data['rec_acc_weights'], data['rec_acc_weights']
        )
        now = data['now']
        observer.now = datetime.datetime.fromisoformat(now) if now else None
        observer.recorder = SeriesRecorder.from_json(data['time_series'])
        return observer
