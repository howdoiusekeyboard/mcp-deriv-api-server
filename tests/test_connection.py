from unittest.mock import patch

import pytest

from deriv_mcp.connection import TICK_BUFFER_SIZE, DerivAPIManager
from tests.conftest import MockObservable, make_mock_api


class TestDerivAPIManagerConnect:
    async def test_connect_authorizes(self):
        api = make_mock_api()
        with patch("deriv_mcp.connection.DerivAPI", return_value=api):
            manager = DerivAPIManager(app_id="1089", api_token="test_token")
            await manager.connect()

        api.authorize.assert_awaited_once_with("test_token")
        assert manager._authorized is True
        assert manager._connected_event.is_set()

    async def test_disconnect_clears_state(self):
        api = make_mock_api()
        with patch("deriv_mcp.connection.DerivAPI", return_value=api):
            manager = DerivAPIManager(app_id="1089", api_token="test_token")
            await manager.connect()
            await manager.disconnect()

        api.clear.assert_awaited_once()
        assert manager._api is None
        assert manager._authorized is False
        assert not manager._connected_event.is_set()


class TestDerivAPIManagerEnsureConnected:
    async def test_returns_api_when_connected(self):
        api = make_mock_api()
        with patch("deriv_mcp.connection.DerivAPI", return_value=api):
            manager = DerivAPIManager(app_id="1089", api_token="test_token")
            await manager.connect()
            result = await manager.ensure_connected()

        assert result is api

    async def test_raises_when_shutting_down(self):
        from mcp.shared.exceptions import McpError

        manager = DerivAPIManager(app_id="1089", api_token="test_token")
        manager._shutting_down = True

        with pytest.raises(McpError, match="shutting down"):
            await manager.ensure_connected()


class TestTickSubscriptions:
    async def test_subscribe_creates_buffer(self):
        observable = MockObservable()
        api = make_mock_api(observable)

        with patch("deriv_mcp.connection.DerivAPI", return_value=api):
            manager = DerivAPIManager(app_id="1089", api_token="test_token")
            await manager.connect()
            await manager.subscribe_ticks("R_100")

        assert "R_100" in manager._tick_buffers
        assert "R_100" in manager._active_subscriptions
        api.subscribe.assert_awaited_once_with({"ticks": "R_100"})

    async def test_subscribe_is_idempotent(self):
        observable = MockObservable()
        api = make_mock_api(observable)

        with patch("deriv_mcp.connection.DerivAPI", return_value=api):
            manager = DerivAPIManager(app_id="1089", api_token="test_token")
            await manager.connect()
            await manager.subscribe_ticks("R_100")
            await manager.subscribe_ticks("R_100")

        assert api.subscribe.await_count == 1

    async def test_tick_handler_appends_to_buffer(self):
        observable = MockObservable()
        api = make_mock_api(observable)

        with patch("deriv_mcp.connection.DerivAPI", return_value=api):
            manager = DerivAPIManager(app_id="1089", api_token="test_token")
            await manager.connect()
            await manager.subscribe_ticks("R_100")

        observable.on_next_callback(
            {
                "tick": {"symbol": "R_100", "epoch": 1713300000, "quote": 1234.56},
                "subscription": {"id": "sub_123"},
            }
        )

        ticks = manager.get_tick_buffer("R_100")
        assert len(ticks) == 1
        assert ticks[0].quote == 1234.56
        assert ticks[0].symbol == "R_100"

    async def test_buffer_bounded_to_max_size(self):
        observable = MockObservable()
        api = make_mock_api(observable)

        with patch("deriv_mcp.connection.DerivAPI", return_value=api):
            manager = DerivAPIManager(app_id="1089", api_token="test_token")
            await manager.connect()
            await manager.subscribe_ticks("R_100")

        for i in range(TICK_BUFFER_SIZE + 20):
            observable.on_next_callback(
                {
                    "tick": {"symbol": "R_100", "epoch": 1713300000 + i, "quote": float(i)},
                }
            )

        ticks = manager.get_tick_buffer("R_100")
        assert len(ticks) == TICK_BUFFER_SIZE
        assert ticks[0].quote == 20.0

    async def test_unsubscribe_clears_state(self):
        observable = MockObservable()
        api = make_mock_api(observable)

        with patch("deriv_mcp.connection.DerivAPI", return_value=api):
            manager = DerivAPIManager(app_id="1089", api_token="test_token")
            await manager.connect()
            await manager.subscribe_ticks("R_100")
            manager._active_subscriptions["R_100"].subscription_id = "sub_123"
            await manager.unsubscribe_ticks("R_100")

        assert "R_100" not in manager._active_subscriptions
        assert "R_100" not in manager._tick_buffers
        api.forget.assert_awaited_once_with("sub_123")

    async def test_get_tick_buffer_empty_when_not_subscribed(self):
        manager = DerivAPIManager(app_id="1089", api_token="test_token")
        assert manager.get_tick_buffer("R_100") == []
