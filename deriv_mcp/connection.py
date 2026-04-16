from __future__ import annotations

import asyncio
import contextlib
import logging
from collections import deque
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from deriv_api import DerivAPI
from mcp.shared.exceptions import McpError
from mcp.types import INTERNAL_ERROR, ErrorData

from deriv_mcp.types import TickData

if TYPE_CHECKING:
    from collections.abc import Callable

    from reactivex.disposable import Disposable

logger = logging.getLogger(__name__)

TICK_BUFFER_SIZE = 50
MAX_RECONNECT_DELAY = 30.0
RECONNECT_TIMEOUT = 60.0


@dataclass
class SubscriptionState:
    """Tracks an active tick subscription for recovery after reconnection."""

    symbol: str
    subscription_id: str | None = None
    disposable: Disposable | None = None


class DerivAPIManager:
    """Singleton connection manager for the Deriv WebSocket API.

    Maintains a single DerivAPI instance shared across all MCP tool calls.
    Handles authorization, reconnection with exponential backoff, and
    automatic recovery of tick subscriptions after connection loss.
    """

    def __init__(self, app_id: str, api_token: str) -> None:
        self._app_id = app_id
        self._api_token = api_token
        self._api: DerivAPI | None = None
        self._authorized: bool = False
        self._shutting_down: bool = False
        self._connected_event: asyncio.Event = asyncio.Event()
        self._tick_buffers: dict[str, deque[TickData]] = {}
        self._active_subscriptions: dict[str, SubscriptionState] = {}
        self._reconnect_task: asyncio.Task[None] | None = None
        self._sanity_disposable: Disposable | None = None

    async def connect(self) -> None:
        """Establish the WebSocket connection and authorize."""
        self._shutting_down = False
        self._api = DerivAPI(app_id=self._app_id)
        self._subscribe_to_errors()
        await self._authorize()
        self._connected_event.set()
        logger.info("Connected and authorized to Deriv API (app_id=%s)", self._app_id)

    async def disconnect(self) -> None:
        """Gracefully shut down: forget subscriptions, close the socket."""
        self._shutting_down = True
        self._connected_event.clear()

        if self._reconnect_task and not self._reconnect_task.done():
            self._reconnect_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._reconnect_task
            self._reconnect_task = None

        if self._sanity_disposable:
            self._sanity_disposable.dispose()
            self._sanity_disposable = None

        await self._cleanup_subscriptions()

        if self._api:
            with contextlib.suppress(Exception):
                await self._api.clear()
            self._api = None

        self._authorized = False
        logger.info("Disconnected from Deriv API")

    async def ensure_connected(self) -> DerivAPI:
        """Return the live API instance, waiting for reconnection if needed."""
        if self._api and self._authorized:
            return self._api

        if self._shutting_down:
            raise McpError(ErrorData(code=INTERNAL_ERROR, message="Server is shutting down"))

        try:
            await asyncio.wait_for(self._connected_event.wait(), timeout=RECONNECT_TIMEOUT)
        except TimeoutError:
            raise McpError(
                ErrorData(code=INTERNAL_ERROR, message="Connection unavailable after timeout")
            ) from None

        if self._api is None:
            raise McpError(ErrorData(code=INTERNAL_ERROR, message="Connection lost"))
        return self._api

    async def _authorize(self) -> None:
        """Authorize the connection with the API token."""
        if self._api and self._api_token:
            await self._api.authorize(self._api_token)
            self._authorized = True

    def _subscribe_to_errors(self) -> None:
        """Listen for connection errors via the DerivAPI sanity_errors Subject."""
        if self._sanity_disposable:
            self._sanity_disposable.dispose()

        if self._api:
            self._sanity_disposable = self._api.sanity_errors.subscribe(
                on_next=self._schedule_error_handler
            )

    def _schedule_error_handler(self, error: Any) -> None:
        """Schedule the async error handler from the sync RxPY callback."""
        if self._shutting_down:
            return
        loop = asyncio.get_running_loop()
        loop.create_task(self._on_connection_error(error))

    async def _on_connection_error(self, error: Any) -> None:
        """Handle a connection drop -- trigger reconnection."""
        logger.warning("Connection error detected: %s", error)
        self._authorized = False
        self._connected_event.clear()

        if self._reconnect_task is None or self._reconnect_task.done():
            self._reconnect_task = asyncio.create_task(self._reconnect_loop())

    async def _reconnect_loop(self) -> None:
        """Reconnect with exponential backoff. Runs until success or shutdown."""
        delay = 1.0
        attempt = 0

        while not self._shutting_down:
            attempt += 1
            logger.info("Reconnection attempt %d (delay=%.1fs)", attempt, delay)

            try:
                if self._api:
                    with contextlib.suppress(Exception):
                        await self._api.clear()

                self._api = DerivAPI(app_id=self._app_id)
                self._subscribe_to_errors()
                await self._authorize()
                await self._recover_subscriptions()
                self._connected_event.set()
                logger.info("Reconnected successfully after %d attempts", attempt)
                return
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning("Reconnection attempt %d failed", attempt, exc_info=True)
                await asyncio.sleep(delay)
                delay = min(delay * 2, MAX_RECONNECT_DELAY)

    # --- Tick subscription methods ---

    async def subscribe_ticks(self, symbol: str) -> None:
        """Subscribe to tick stream for a symbol. Idempotent."""
        if symbol in self._active_subscriptions:
            return

        api = await self.ensure_connected()
        self._tick_buffers[symbol] = deque(maxlen=TICK_BUFFER_SIZE)

        observable = await api.subscribe({"ticks": symbol})
        sub_state = SubscriptionState(symbol=symbol)

        disposable = observable.subscribe(on_next=self._make_tick_handler(symbol, sub_state))
        sub_state.disposable = disposable
        self._active_subscriptions[symbol] = sub_state
        logger.info("Subscribed to ticks for %s", symbol)

    async def unsubscribe_ticks(self, symbol: str) -> None:
        """Unsubscribe from a symbol's tick stream and clear its buffer."""
        state = self._active_subscriptions.pop(symbol, None)
        if state is None:
            return

        if state.disposable:
            state.disposable.dispose()

        if state.subscription_id and self._api:
            try:
                await self._api.forget(state.subscription_id)
            except Exception:
                logger.debug("Failed to forget subscription %s", state.subscription_id)

        self._tick_buffers.pop(symbol, None)
        logger.info("Unsubscribed from ticks for %s", symbol)

    async def unsubscribe_all_ticks(self) -> None:
        """Unsubscribe from all tick streams."""
        if self._api:
            with contextlib.suppress(Exception):
                await self._api.forget_all("ticks")

        for state in self._active_subscriptions.values():
            if state.disposable:
                state.disposable.dispose()

        self._active_subscriptions.clear()
        self._tick_buffers.clear()
        logger.info("Unsubscribed from all tick streams")

    def get_tick_buffer(self, symbol: str) -> list[TickData]:
        """Return a snapshot of the tick buffer for a symbol. No API call."""
        buf = self._tick_buffers.get(symbol)
        if buf is None:
            return []
        return list(buf)

    def _make_tick_handler(
        self, symbol: str, sub_state: SubscriptionState
    ) -> Callable[[dict], None]:
        """Create an on_next callback for a tick subscription.

        Uses a factory to avoid closure-variable capture issues in loops.
        """

        def on_tick(data: dict) -> None:
            if "tick" in data:
                tick = data["tick"]
                self._tick_buffers[symbol].append(
                    TickData(
                        symbol=tick["symbol"],
                        epoch=tick["epoch"],
                        quote=tick["quote"],
                        ask=tick.get("ask"),
                        bid=tick.get("bid"),
                    )
                )
            if "subscription" in data:
                sub_state.subscription_id = data["subscription"]["id"]

        return on_tick

    async def _recover_subscriptions(self) -> None:
        """Re-subscribe to all active tick streams after reconnection."""
        symbols = list(self._active_subscriptions.keys())
        if not symbols:
            return

        logger.info("Recovering %d tick subscriptions", len(symbols))

        for state in self._active_subscriptions.values():
            if state.disposable:
                state.disposable.dispose()

        for symbol in symbols:
            try:
                observable = await self._api.subscribe({"ticks": symbol})
                sub_state = SubscriptionState(symbol=symbol)
                disposable = observable.subscribe(
                    on_next=self._make_tick_handler(symbol, sub_state)
                )
                sub_state.disposable = disposable
                self._active_subscriptions[symbol] = sub_state
                logger.info("Recovered tick subscription for %s", symbol)
            except Exception:
                logger.warning("Failed to recover subscription for %s", symbol, exc_info=True)
                self._active_subscriptions.pop(symbol, None)
                self._tick_buffers.pop(symbol, None)

    async def _cleanup_subscriptions(self) -> None:
        """Dispose all subscription observables and clear state."""
        for state in self._active_subscriptions.values():
            if state.disposable:
                state.disposable.dispose()
        self._active_subscriptions.clear()
        self._tick_buffers.clear()
