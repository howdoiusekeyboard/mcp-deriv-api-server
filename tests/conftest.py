from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock

import pytest


@dataclass
class MockObservable:
    """Fake RxPY Observable that captures the on_next callback."""

    on_next_callback: callable = None
    disposed: bool = False

    def subscribe(self, on_next=None, on_error=None, on_completed=None):
        self.on_next_callback = on_next
        disposable = MagicMock()
        disposable.dispose = MagicMock(side_effect=lambda: setattr(self, "disposed", True))
        return disposable


def make_mock_api(observable: MockObservable | None = None) -> MagicMock:
    """Create a mock DerivAPI instance with standard async methods."""
    api = MagicMock()
    api.authorize = AsyncMock()
    api.balance = AsyncMock(
        return_value={"balance": {"balance": 1000, "currency": "USD", "loginid": "CR123"}}
    )
    api.active_symbols = AsyncMock(return_value={"active_symbols": []})
    api.portfolio = AsyncMock(return_value={"portfolio": {"contracts": []}})
    api.proposal = AsyncMock(
        return_value={
            "proposal": {
                "proposal_id": "p1",
                "ask_price": 10,
                "payout": 19,
                "spot": 1234,
                "spot_time": 1713300000,
                "display_value": "10.00",
            }
        }
    )
    api.subscribe = AsyncMock(return_value=observable or MockObservable())
    api.forget = AsyncMock()
    api.forget_all = AsyncMock()
    api.clear = AsyncMock()
    api.ping = AsyncMock(return_value={"ping": "pong"})

    # sanity_errors is an RxPY Subject — mock it
    api.sanity_errors = MagicMock()
    api.sanity_errors.subscribe = MagicMock(return_value=MagicMock())

    return api


@pytest.fixture
def mock_observable():
    return MockObservable()


@pytest.fixture
def mock_api(mock_observable):
    return make_mock_api(mock_observable)
