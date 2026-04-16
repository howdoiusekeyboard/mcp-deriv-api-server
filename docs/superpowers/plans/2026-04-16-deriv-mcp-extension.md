# Deriv MCP Server Extension — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend the Deriv MCP server with a singleton connection manager (auto-reconnect + subscription recovery), 7 MCP tools across market/account/trading domains, Pydantic v2 types, structured error handling, and modern Python tooling.

**Architecture:** Shared `DerivAPIManager` singleton initialized via FastMCP lifespan context. All tools access the single WebSocket connection through `ctx.request_context.lifespan_context.connection`. Tick streaming uses a subscribe+poll pattern with bounded `collections.deque(maxlen=50)` per symbol. Reconnection uses exponential backoff with automatic subscription recovery.

**Tech Stack:** Python 3.11, FastMCP (mcp[cli] 1.2.0), python-deriv-api 0.1.6, Pydantic v2, RxPY 4.x, ruff, pytest + pytest-asyncio

**Design Spec:** `docs/superpowers/specs/2026-04-16-deriv-mcp-extension-design.md`

---

## File Structure

```
deriv_mcp/
  __init__.py          # Package marker, version string
  server.py            # FastMCP creation, lifespan context, tool registration, main()
  connection.py        # DerivAPIManager class + SubscriptionState dataclass
  tools/
    __init__.py        # Empty, marks subpackage
    market.py          # get_active_symbols, subscribe_ticks, get_latest_ticks, unsubscribe_ticks
    account.py         # get_account_balance, get_portfolio
    trading.py         # get_proposal
  types.py             # Pydantic v2 input/output models
  errors.py            # Deriv API error -> McpError mapping + context manager
tests/
  __init__.py
  conftest.py          # Shared fixtures (mock DerivAPI, mock manager)
  test_types.py        # Pydantic model validation tests
  test_errors.py       # Error mapping tests
  test_connection.py   # Connection manager unit tests
pyproject.toml         # Enhanced with ruff, dependency-groups, scripts
Dockerfile             # Updated for new package structure
.env.example           # Updated with both vars documented
README.md              # Full rewrite
```

Files to **delete** after migration: `server.py` (root-level), `services/` directory.

---

## Task 1: Project Scaffolding & Configuration

**Files:**
- Modify: `pyproject.toml`
- Create: `deriv_mcp/__init__.py`
- Create: `deriv_mcp/tools/__init__.py`
- Create: `tests/__init__.py`

- [ ] **Step 1: Enhance pyproject.toml**

```toml
[project]
name = "deriv-api-mcp"
version = "0.2.0"
description = "MCP server for the Deriv trading API — real-time ticks, contract proposals, and portfolio management"
readme = "README.md"
requires-python = ">=3.11"
dependencies = [
    "mcp[cli]>=1.2.0,<2.0",
    "python-deriv-api>=0.1.6",
    "python-dotenv>=1.0.1",
]

[project.scripts]
deriv-mcp = "deriv_mcp.server:main"

[dependency-groups]
dev = ["ruff>=0.9", "pytest>=8.0", "pytest-asyncio>=0.24"]

[tool.ruff]
target-version = "py311"
line-length = 100

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B", "SIM", "TCH"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
```

- [ ] **Step 2: Create package structure**

Create `deriv_mcp/__init__.py`:
```python
"""MCP server for the Deriv trading API."""

__version__ = "0.2.0"
```

Create `deriv_mcp/tools/__init__.py`:
```python
"""Deriv MCP tool implementations."""
```

Create `tests/__init__.py` (empty file).

- [ ] **Step 3: Install dependencies**

Run: `uv sync --all-groups`
Expected: Resolves and installs all deps including dev group (ruff, pytest, pytest-asyncio).

- [ ] **Step 4: Verify ruff runs**

Run: `uv run ruff check deriv_mcp/`
Expected: No errors (only __init__.py files exist).

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml deriv_mcp/ tests/__init__.py
git commit -m "scaffold: create deriv_mcp package structure with modern tooling"
```

---

## Task 2: Pydantic Type Definitions

**Files:**
- Create: `deriv_mcp/types.py`
- Create: `tests/test_types.py`

- [ ] **Step 1: Write test_types.py**

```python
from deriv_mcp.types import (
    ActiveSymbol,
    BalanceInfo,
    PortfolioContract,
    ProposalInfo,
    ProposalRequest,
    TickData,
)


class TestTickData:
    def test_parse_full_tick(self):
        tick = TickData(symbol="R_100", epoch=1713300000, quote=1234.56, ask=1234.57, bid=1234.55)
        assert tick.symbol == "R_100"
        assert tick.quote == 1234.56
        assert tick.ask == 1234.57

    def test_parse_tick_without_ask_bid(self):
        tick = TickData(symbol="R_100", epoch=1713300000, quote=1234.56)
        assert tick.ask is None
        assert tick.bid is None

    def test_ignores_extra_fields(self):
        tick = TickData(symbol="R_100", epoch=1713300000, quote=1.0, extra_field="ignored")
        assert not hasattr(tick, "extra_field")


class TestProposalRequest:
    def test_valid_request(self):
        req = ProposalRequest(
            contract_type="CALL",
            symbol="R_100",
            duration=5,
            duration_unit="m",
            amount=10.0,
        )
        assert req.currency == "USD"
        assert req.basis == "stake"

    def test_rejects_zero_duration(self):
        import pytest

        with pytest.raises(Exception):
            ProposalRequest(
                contract_type="CALL",
                symbol="R_100",
                duration=0,
                duration_unit="m",
                amount=10.0,
            )

    def test_rejects_negative_amount(self):
        import pytest

        with pytest.raises(Exception):
            ProposalRequest(
                contract_type="CALL",
                symbol="R_100",
                duration=5,
                duration_unit="m",
                amount=-1.0,
            )


class TestActiveSymbol:
    def test_parse_from_api_response(self):
        data = {
            "symbol": "R_100",
            "display_name": "Volatility 100 Index",
            "market": "synthetic_index",
            "market_display_name": "Synthetic Indices",
            "pip": 0.01,
            "exchange_is_open": 1,
        }
        sym = ActiveSymbol.model_validate(data)
        assert sym.symbol == "R_100"
        assert sym.display_name == "Volatility 100 Index"


class TestBalanceInfo:
    def test_parse_balance(self):
        data = {"balance": 10000.00, "currency": "USD", "loginid": "CR123456"}
        info = BalanceInfo.model_validate(data)
        assert info.balance == 10000.00
        assert info.loginid == "CR123456"


class TestPortfolioContract:
    def test_parse_contract(self):
        data = {
            "contract_id": 12345,
            "contract_type": "CALL",
            "symbol": "R_100",
            "buy_price": 10.0,
            "payout": 19.5,
            "expiry_time": 1713300000,
        }
        contract = PortfolioContract.model_validate(data)
        assert contract.contract_id == 12345
        assert contract.payout == 19.5


class TestProposalInfo:
    def test_parse_proposal(self):
        data = {
            "proposal_id": "abc123",
            "ask_price": 10.0,
            "payout": 19.5,
            "spot": 1234.56,
            "spot_time": 1713300000,
            "display_value": "10.00",
        }
        info = ProposalInfo.model_validate(data)
        assert info.ask_price == 10.0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_types.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'deriv_mcp.types'`

- [ ] **Step 3: Write types.py**

```python
from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class ProposalRequest(BaseModel):
    """Input parameters for pricing a contract."""

    contract_type: str
    symbol: str
    duration: int = Field(gt=0)
    duration_unit: str
    amount: float = Field(gt=0)
    currency: str = "USD"
    basis: str = "stake"


class ActiveSymbol(BaseModel):
    """A tradable symbol from the Deriv platform."""

    model_config = ConfigDict(extra="ignore")

    symbol: str
    display_name: str
    market: str
    market_display_name: str
    pip: float


class BalanceInfo(BaseModel):
    """Account balance details."""

    model_config = ConfigDict(extra="ignore")

    balance: float
    currency: str
    loginid: str


class PortfolioContract(BaseModel):
    """An active contract in the portfolio."""

    model_config = ConfigDict(extra="ignore")

    contract_id: int
    contract_type: str
    symbol: str
    buy_price: float
    payout: float
    expiry_time: int


class ProposalInfo(BaseModel):
    """Pricing details for a proposed contract."""

    model_config = ConfigDict(extra="ignore")

    proposal_id: str
    ask_price: float
    payout: float
    spot: float
    spot_time: int
    display_value: str


class TickData(BaseModel):
    """A single price tick."""

    model_config = ConfigDict(extra="ignore")

    symbol: str
    epoch: int
    quote: float
    ask: float | None = None
    bid: float | None = None
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_types.py -v`
Expected: All 9 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add deriv_mcp/types.py tests/test_types.py
git commit -m "feat: add Pydantic v2 type definitions for all Deriv API models"
```

---

## Task 3: Error Handling

**Files:**
- Create: `deriv_mcp/errors.py`
- Create: `tests/test_errors.py`

- [ ] **Step 1: Write test_errors.py**

```python
import pytest
from mcp.shared.exceptions import McpError
from mcp.types import INTERNAL_ERROR, INVALID_PARAMS, INVALID_REQUEST

from deriv_mcp.errors import deriv_error_to_mcp, handle_deriv_errors


class FakeResponseError(Exception):
    """Mimics deriv_api.errors.ResponseError for testing."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


class TestDerivErrorToMcp:
    def test_authorization_required(self):
        err = FakeResponseError("AuthorizationRequired", "Please authorize")
        mcp_err = deriv_error_to_mcp(err)
        assert mcp_err.error.code == INVALID_REQUEST
        assert "authorize" in mcp_err.error.message.lower()

    def test_invalid_token(self):
        err = FakeResponseError("InvalidToken", "Token is invalid")
        mcp_err = deriv_error_to_mcp(err)
        assert mcp_err.error.code == INVALID_REQUEST

    def test_invalid_symbol(self):
        err = FakeResponseError("InvalidSymbol", "Symbol not found")
        mcp_err = deriv_error_to_mcp(err)
        assert mcp_err.error.code == INVALID_PARAMS

    def test_invalid_contract_type(self):
        err = FakeResponseError("InvalidContractType", "Bad contract type")
        mcp_err = deriv_error_to_mcp(err)
        assert mcp_err.error.code == INVALID_PARAMS

    def test_rate_limit(self):
        err = FakeResponseError("RateLimit", "Too many requests")
        mcp_err = deriv_error_to_mcp(err)
        assert mcp_err.error.code == INTERNAL_ERROR

    def test_unknown_error_code(self):
        err = FakeResponseError("SomeNewError", "Unknown thing happened")
        mcp_err = deriv_error_to_mcp(err)
        assert mcp_err.error.code == INTERNAL_ERROR
        assert "Unknown thing happened" in mcp_err.error.message


class TestHandleDerivErrors:
    async def test_passes_through_on_success(self):
        async with handle_deriv_errors():
            result = 42
        assert result == 42

    async def test_catches_response_error_and_raises_mcp_error(self):
        from deriv_api import APIError

        with pytest.raises(McpError) as exc_info:
            async with handle_deriv_errors():
                raise APIError("Something broke")
        assert exc_info.value.error.code == INTERNAL_ERROR
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_errors.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'deriv_mcp.errors'`

- [ ] **Step 3: Write errors.py**

```python
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from mcp.shared.exceptions import McpError
from mcp.types import INTERNAL_ERROR, INVALID_PARAMS, INVALID_REQUEST, ErrorData
from websockets.exceptions import ConnectionClosed

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

from deriv_api import APIError

logger = logging.getLogger(__name__)

_AUTH_ERRORS = frozenset({"AuthorizationRequired", "InvalidToken"})
_PARAM_ERRORS = frozenset({"InvalidSymbol", "InvalidContractType", "InputValidationFailed"})
_RATE_ERRORS = frozenset({"RateLimit"})


def deriv_error_to_mcp(error: Exception) -> McpError:
    """Map a Deriv API error to an McpError with the appropriate JSON-RPC code."""
    code_attr = getattr(error, "code", None)
    message = getattr(error, "message", str(error))

    if code_attr in _AUTH_ERRORS:
        return McpError(ErrorData(code=INVALID_REQUEST, message=message))
    if code_attr in _PARAM_ERRORS:
        return McpError(ErrorData(code=INVALID_PARAMS, message=message))
    if code_attr in _RATE_ERRORS:
        return McpError(ErrorData(code=INTERNAL_ERROR, message=f"Rate limited: {message}"))
    return McpError(ErrorData(code=INTERNAL_ERROR, message=message))


@asynccontextmanager
async def handle_deriv_errors() -> AsyncIterator[None]:
    """Context manager that catches Deriv API errors and re-raises as McpError."""
    try:
        yield
    except McpError:
        raise
    except APIError as e:
        logger.warning("Deriv API error: %s", e)
        raise McpError(ErrorData(code=INTERNAL_ERROR, message=str(e))) from e
    except ConnectionClosed as e:
        logger.warning("WebSocket connection lost: %s", e)
        raise McpError(
            ErrorData(code=INTERNAL_ERROR, message="Connection lost, please retry")
        ) from e
    except Exception as e:
        code_attr = getattr(e, "code", None)
        if code_attr:
            raise deriv_error_to_mcp(e) from e
        logger.exception("Unexpected error in Deriv API call")
        raise McpError(ErrorData(code=INTERNAL_ERROR, message=str(e))) from e
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_errors.py -v`
Expected: All 8 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add deriv_mcp/errors.py tests/test_errors.py
git commit -m "feat: add Deriv-to-MCP error mapping with structured error codes"
```

---

## Task 4: Connection Manager — Core Lifecycle

**Files:**
- Create: `deriv_mcp/connection.py`

This task implements connect, disconnect, ensure_connected, and authorize. Reconnection and tick subscriptions come in Tasks 5 and 6.

- [ ] **Step 1: Write connection.py with core lifecycle**

```python
from __future__ import annotations

import asyncio
import logging
from collections import deque
from dataclasses import dataclass, field
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from deriv_api import DerivAPI
from mcp.shared.exceptions import McpError
from mcp.types import INTERNAL_ERROR, ErrorData

from deriv_mcp.types import TickData

if TYPE_CHECKING:
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
            try:
                await self._reconnect_task
            except asyncio.CancelledError:
                pass
            self._reconnect_task = None

        if self._sanity_disposable:
            self._sanity_disposable.dispose()
            self._sanity_disposable = None

        await self._cleanup_subscriptions()

        if self._api:
            try:
                await self._api.clear()
            except Exception:
                pass
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
        except asyncio.TimeoutError:
            raise McpError(
                ErrorData(code=INTERNAL_ERROR, message="Connection unavailable after timeout")
            )

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
        """Handle a connection drop — trigger reconnection."""
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
                    try:
                        await self._api.clear()
                    except Exception:
                        pass

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

    # --- Tick subscription methods (Task 6) ---

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
            try:
                await self._api.forget_all("ticks")
            except Exception:
                pass

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
```

- [ ] **Step 2: Verify the module imports cleanly**

Run: `uv run python -c "from deriv_mcp.connection import DerivAPIManager; print('OK')"`
Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add deriv_mcp/connection.py
git commit -m "feat: add DerivAPIManager with reconnection and tick subscription recovery"
```

---

## Task 5: Server Lifespan & Tool Registration

**Files:**
- Create: `deriv_mcp/server.py`

- [ ] **Step 1: Write server.py**

```python
from __future__ import annotations

import logging
import os
import sys
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING

from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

from deriv_mcp.connection import DerivAPIManager

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

load_dotenv()

logger = logging.getLogger(__name__)


@dataclass
class AppContext:
    """Lifespan state shared across all tool calls."""

    connection: DerivAPIManager


def _read_config() -> tuple[str, str]:
    """Read and validate environment configuration."""
    api_token = os.getenv("DERIV_API_TOKEN", "")
    if not api_token:
        msg = (
            "DERIV_API_TOKEN is required. "
            "Get one at https://app.deriv.com/account/api-token"
        )
        raise ValueError(msg)

    app_id = os.getenv("DERIV_APP_ID", "1089")
    return app_id, api_token


@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    """Initialize the shared connection manager for the server lifetime."""
    app_id, api_token = _read_config()
    manager = DerivAPIManager(app_id=app_id, api_token=api_token)
    await manager.connect()
    try:
        yield AppContext(connection=manager)
    finally:
        await manager.disconnect()


mcp = FastMCP("deriv-api-mcp", lifespan=app_lifespan)

# Register tools — each module's register() adds its @mcp.tool() handlers
from deriv_mcp.tools import account, market, trading  # noqa: E402

account.register(mcp)
market.register(mcp)
trading.register(mcp)


def main() -> None:
    """Entry point for the MCP server."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        stream=sys.stderr,
    )
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Verify module loads (will fail until tools exist — expected)**

Run: `uv run python -c "from deriv_mcp.server import mcp; print(type(mcp))"`
Expected: Will fail because tools/market.py etc. don't exist yet. That's fine — they're created in the next tasks.

- [ ] **Step 3: Commit**

```bash
git add deriv_mcp/server.py
git commit -m "feat: add server entry point with lifespan-managed connection"
```

---

## Task 6: Market Tools

**Files:**
- Create: `deriv_mcp/tools/market.py`

- [ ] **Step 1: Write tools/market.py**

```python
from __future__ import annotations

from mcp.server.fastmcp import Context

from deriv_mcp.connection import DerivAPIManager
from deriv_mcp.errors import handle_deriv_errors
from deriv_mcp.types import ActiveSymbol, TickData


def _get_manager(ctx: Context) -> DerivAPIManager:
    return ctx.request_context.lifespan_context.connection


def register(mcp):
    """Register market data tools with the MCP server."""

    @mcp.tool()
    async def get_active_symbols(ctx: Context, product_type: str = "basic") -> list[dict]:
        """Get the list of active trading symbols on the Deriv platform.

        Args:
            product_type: Type of product to filter by (default: "basic").
        """
        manager = _get_manager(ctx)
        api = await manager.ensure_connected()

        async with handle_deriv_errors():
            response = await api.active_symbols(
                {"active_symbols": "brief", "product_type": product_type}
            )

        symbols = [
            ActiveSymbol.model_validate(s).model_dump()
            for s in response.get("active_symbols", [])
        ]
        return symbols

    @mcp.tool()
    async def subscribe_ticks(ctx: Context, symbol: str) -> str:
        """Subscribe to real-time tick data for a trading symbol.

        Starts a background WebSocket listener that stores ticks in a bounded
        buffer (max 50 per symbol). Use get_latest_ticks to read the data.

        Args:
            symbol: The trading symbol to subscribe to (e.g. "R_100").
        """
        manager = _get_manager(ctx)

        async with handle_deriv_errors():
            await manager.subscribe_ticks(symbol)

        return f"Subscribed to {symbol}. Use get_latest_ticks to read tick data."

    @mcp.tool()
    async def get_latest_ticks(
        ctx: Context, symbol: str, count: int = 10
    ) -> list[dict]:
        """Get the latest ticks from the buffer for a subscribed symbol.

        Returns cached data from the ring buffer — no API call is made.
        Call subscribe_ticks first to start receiving data.

        Args:
            symbol: The trading symbol (e.g. "R_100").
            count: Number of ticks to return (1-50, default 10).
        """
        count = max(1, min(count, 50))
        manager = _get_manager(ctx)
        ticks = manager.get_tick_buffer(symbol)

        if not ticks:
            return []

        return [t.model_dump() for t in ticks[-count:]]

    @mcp.tool()
    async def unsubscribe_ticks(ctx: Context, symbol: str) -> str:
        """Stop receiving tick data for a symbol and clear its buffer.

        Args:
            symbol: The trading symbol to unsubscribe from.
        """
        manager = _get_manager(ctx)

        async with handle_deriv_errors():
            await manager.unsubscribe_ticks(symbol)

        return f"Unsubscribed from {symbol}."
```

- [ ] **Step 2: Commit**

```bash
git add deriv_mcp/tools/market.py
git commit -m "feat: add market tools — active symbols, tick subscribe/poll/unsubscribe"
```

---

## Task 7: Account Tools

**Files:**
- Create: `deriv_mcp/tools/account.py`

- [ ] **Step 1: Write tools/account.py**

```python
from __future__ import annotations

from mcp.server.fastmcp import Context

from deriv_mcp.connection import DerivAPIManager
from deriv_mcp.errors import handle_deriv_errors
from deriv_mcp.types import BalanceInfo, PortfolioContract


def _get_manager(ctx: Context) -> DerivAPIManager:
    return ctx.request_context.lifespan_context.connection


def register(mcp):
    """Register account tools with the MCP server."""

    @mcp.tool()
    async def get_account_balance(ctx: Context) -> dict:
        """Get the current account balance for the authenticated Deriv account."""
        manager = _get_manager(ctx)
        api = await manager.ensure_connected()

        async with handle_deriv_errors():
            response = await api.balance()

        return BalanceInfo.model_validate(response["balance"]).model_dump()

    @mcp.tool()
    async def get_portfolio(ctx: Context) -> list[dict]:
        """Get the list of active contracts in the portfolio.

        Returns all currently open positions with contract details,
        buy price, potential payout, and expiry time.
        """
        manager = _get_manager(ctx)
        api = await manager.ensure_connected()

        async with handle_deriv_errors():
            response = await api.portfolio({"portfolio": 1})

        contracts = response.get("portfolio", {}).get("contracts", [])
        return [PortfolioContract.model_validate(c).model_dump() for c in contracts]
```

- [ ] **Step 2: Commit**

```bash
git add deriv_mcp/tools/account.py
git commit -m "feat: add account tools — balance and portfolio management"
```

---

## Task 8: Trading Tools

**Files:**
- Create: `deriv_mcp/tools/trading.py`

- [ ] **Step 1: Write tools/trading.py**

```python
from __future__ import annotations

from mcp.server.fastmcp import Context

from deriv_mcp.connection import DerivAPIManager
from deriv_mcp.errors import handle_deriv_errors
from deriv_mcp.types import ProposalInfo, ProposalRequest


def _get_manager(ctx: Context) -> DerivAPIManager:
    return ctx.request_context.lifespan_context.connection


def register(mcp):
    """Register trading tools with the MCP server."""

    @mcp.tool()
    async def get_proposal(
        ctx: Context,
        contract_type: str,
        symbol: str,
        duration: int,
        duration_unit: str,
        amount: float,
        currency: str = "USD",
        basis: str = "stake",
    ) -> dict:
        """Get a price quote for a trading contract.

        Returns the ask price, potential payout, current spot price, and
        other pricing details for the specified contract parameters.

        Args:
            contract_type: Type of contract (e.g. "CALL", "PUT", "MULTUP", "MULTDOWN").
            symbol: Trading symbol (e.g. "R_100", "1HZ100V").
            duration: Contract duration value.
            duration_unit: Duration unit — "t" (ticks), "s" (seconds), "m" (minutes),
                "h" (hours), "d" (days).
            amount: Stake or payout amount.
            currency: Currency code (default: "USD").
            basis: "stake" (amount is the cost) or "payout" (amount is the target payout).
        """
        req = ProposalRequest(
            contract_type=contract_type,
            symbol=symbol,
            duration=duration,
            duration_unit=duration_unit,
            amount=amount,
            currency=currency,
            basis=basis,
        )

        manager = _get_manager(ctx)
        api = await manager.ensure_connected()

        async with handle_deriv_errors():
            response = await api.proposal(
                {
                    "proposal": 1,
                    "contract_type": req.contract_type,
                    "symbol": req.symbol,
                    "duration": req.duration,
                    "duration_unit": req.duration_unit,
                    "amount": req.amount,
                    "currency": req.currency,
                    "basis": req.basis,
                }
            )

        return ProposalInfo.model_validate(response["proposal"]).model_dump()
```

- [ ] **Step 2: Commit**

```bash
git add deriv_mcp/tools/trading.py
git commit -m "feat: add trading tools — contract proposal pricing"
```

---

## Task 9: Connection Manager Tests

**Files:**
- Create: `tests/conftest.py`
- Create: `tests/test_connection.py`

- [ ] **Step 1: Write conftest.py**

```python
import asyncio
from collections import deque
from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock

import pytest

from deriv_mcp.connection import DerivAPIManager


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
```

- [ ] **Step 2: Write test_connection.py**

```python
from unittest.mock import patch

import pytest

from deriv_mcp.connection import DerivAPIManager, TICK_BUFFER_SIZE

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

        observable.on_next_callback({
            "tick": {"symbol": "R_100", "epoch": 1713300000, "quote": 1234.56},
            "subscription": {"id": "sub_123"},
        })

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
            observable.on_next_callback({
                "tick": {"symbol": "R_100", "epoch": 1713300000 + i, "quote": float(i)},
            })

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
```

- [ ] **Step 3: Run tests**

Run: `uv run pytest tests/ -v`
Expected: All tests pass (types, errors, and connection tests).

- [ ] **Step 4: Commit**

```bash
git add tests/conftest.py tests/test_connection.py
git commit -m "test: add connection manager tests — lifecycle, subscriptions, buffer bounds"
```

---

## Task 10: Dockerfile & README

**Files:**
- Modify: `Dockerfile`
- Modify: `README.md`
- Modify: `.env.example`

- [ ] **Step 1: Rewrite Dockerfile**

```dockerfile
FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir pip --upgrade

COPY pyproject.toml .
COPY deriv_mcp/ deriv_mcp/

RUN pip install --no-cache-dir .

COPY .env* ./

CMD ["python", "-m", "deriv_mcp.server"]
```

- [ ] **Step 2: Update .env.example**

```env
# Deriv API Configuration
# Get your API token at: https://app.deriv.com/account/api-token
DERIV_API_TOKEN=your_api_token_here

# Application ID (default works for development, register your own at https://api.deriv.com)
DERIV_APP_ID=1089
```

- [ ] **Step 3: Rewrite README.md**

```markdown
# Deriv API MCP Server

An MCP server for the Deriv trading API. Provides real-time market data, portfolio management, and contract pricing through a persistent WebSocket connection.

## Tools

| Tool | Description |
|------|-------------|
| `get_active_symbols` | List tradable symbols on the Deriv platform |
| `get_account_balance` | Current account balance and currency |
| `get_portfolio` | Active contracts and open positions |
| `get_proposal` | Price quote for a trading contract (CALL, PUT, MULTUP, etc.) |
| `subscribe_ticks` | Start receiving real-time tick data for a symbol |
| `get_latest_ticks` | Read the latest ticks from the buffer (no API call) |
| `unsubscribe_ticks` | Stop a tick subscription and free its buffer |

## Configuration

Create a `.env` file in the project root:

```env
DERIV_API_TOKEN=your_token_here
DERIV_APP_ID=1089
```

Get an API token at [app.deriv.com/account/api-token](https://app.deriv.com/account/api-token). The default `APP_ID` (1089) works for development.

## Installation

### Local (uv)

```bash
uv sync
```

### Docker

```bash
docker build -t deriv-api-mcp .
```

## Usage with Claude Desktop

Add to your Claude Desktop config:

**macOS:** `~/Library/Application Support/Claude/claude_desktop_config.json`
**Windows:** `%APPDATA%\Claude\claude_desktop_config.json`

### Local

```json
{
  "mcpServers": {
    "deriv-api-mcp": {
      "command": "uv",
      "args": ["--directory", "/path/to/mcp-deriv-api-server", "run", "python", "-m", "deriv_mcp.server"]
    }
  }
}
```

### Docker

```json
{
  "mcpServers": {
    "deriv-api-mcp": {
      "command": "docker",
      "args": ["run", "--rm", "-i", "--env-file", ".env", "deriv-api-mcp"]
    }
  }
}
```

## Architecture

The server maintains a single persistent WebSocket connection to the Deriv API, shared across all tool calls. Key design decisions:

- **Singleton connection** — One `DerivAPI` instance multiplexes all requests and subscriptions over a single socket, avoiding redundant TLS handshakes and rate limit pressure.
- **Auto-reconnection** — On connection drop, exponential backoff (1s to 30s cap) retries indefinitely until the socket is restored.
- **Subscription recovery** — Active tick subscriptions are automatically re-established after reconnection, keeping the ring buffers populated without manual intervention.
- **Bounded tick buffers** — Each subscribed symbol stores up to 50 ticks in a `collections.deque`, preventing unbounded memory growth from high-frequency streams.

## Development

```bash
uv sync --all-groups
uv run pytest
uv run ruff check deriv_mcp/
```

## License

MIT
```

- [ ] **Step 4: Commit**

```bash
git add Dockerfile README.md .env.example
git commit -m "docs: rewrite README, update Dockerfile and env config for new architecture"
```

---

## Task 11: Delete Old Files & Final Cleanup

**Files:**
- Delete: `server.py` (root-level)
- Delete: `services/__init__.py`
- Delete: `services/tools.py`

- [ ] **Step 1: Remove old source files**

```bash
git rm server.py services/__init__.py services/tools.py
rmdir services 2>/dev/null || true
```

- [ ] **Step 2: Run ruff check and fix**

Run: `uv run ruff check deriv_mcp/ tests/ --fix`
Then: `uv run ruff format deriv_mcp/ tests/`

Fix any remaining issues manually.

- [ ] **Step 3: Run full test suite**

Run: `uv run pytest tests/ -v`
Expected: All tests pass.

- [ ] **Step 4: Verify server starts**

Run: `uv run python -c "from deriv_mcp.server import mcp; print('Server module OK')"`
Expected: `Server module OK`

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "cleanup: remove old server.py and services/, lint and format"
```

---

## Task 12: End-to-End Verification

This is a manual verification task. Requires a valid `DERIV_API_TOKEN` in `.env`.

- [ ] **Step 1: Start the server**

Run: `uv run python -m deriv_mcp.server`
Expected: `Deriv MCP server running` appears on stderr. Server waits for stdio input.

- [ ] **Step 2: Test via Claude Desktop**

Add the server to Claude Desktop config and restart. Verify each tool:

1. Call `get_active_symbols` — returns a list of symbol objects
2. Call `get_account_balance` — returns balance, currency, loginid
3. Call `get_portfolio` — returns list of contracts (may be empty)
4. Call `get_proposal` with: contract_type="CALL", symbol="R_100", duration=5, duration_unit="t", amount=10 — returns pricing
5. Call `subscribe_ticks` with symbol="R_100" — returns confirmation
6. Wait 5 seconds, call `get_latest_ticks` with symbol="R_100" — returns tick data
7. Call `unsubscribe_ticks` with symbol="R_100" — returns confirmation

- [ ] **Step 3: Test error handling**

1. Call `get_proposal` with an invalid symbol (e.g., "INVALID_XYZ") — should return clean error, not stack trace
2. Call `get_latest_ticks` for a symbol not subscribed to — should return empty list

- [ ] **Step 4: Docker build**

Run: `docker build -t deriv-api-mcp .`
Expected: Build succeeds.

---

## Summary

| Task | Focus | Files |
|------|-------|-------|
| 1 | Scaffolding & config | pyproject.toml, __init__.py files |
| 2 | Pydantic types + tests | types.py, test_types.py |
| 3 | Error handling + tests | errors.py, test_errors.py |
| 4 | Connection manager | connection.py |
| 5 | Server lifespan | server.py |
| 6 | Market tools | tools/market.py |
| 7 | Account tools | tools/account.py |
| 8 | Trading tools | tools/trading.py |
| 9 | Connection tests | conftest.py, test_connection.py |
| 10 | Dockerfile + README | Dockerfile, README.md, .env.example |
| 11 | Delete old files + lint | server.py, services/ removed |
| 12 | E2E verification | Manual testing |
