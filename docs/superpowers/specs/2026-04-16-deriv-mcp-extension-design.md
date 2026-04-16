# Deriv MCP Server Extension — Design Spec

## Context

This MCP server bridges Claude with the Deriv trading API. It currently has two tools (get_active_symbols, get_account_balance) with no shared connection management, no input validation, and no error handling. Each tool opens and closes its own WebSocket connection.

We are extending it with real-time tick streaming, contract proposals, and portfolio management. The key architectural challenge is managing a persistent WebSocket connection with subscription recovery — the same pattern Deriv uses in production trading infrastructure.

**Goal:** Demonstrate production-grade WebSocket architecture in a portfolio piece for an Applied AI Engineering role at Deriv.

---

## Module Layout

```
deriv_mcp/
  __init__.py          # Package marker
  server.py            # FastMCP creation, lifespan, tool registration
  connection.py        # DerivAPIManager singleton
  tools/
    __init__.py
    market.py          # get_active_symbols, subscribe_ticks, get_latest_ticks, unsubscribe_ticks
    account.py         # get_account_balance, get_portfolio
    trading.py         # get_proposal
  types.py             # Pydantic v2 models for all inputs/outputs
  errors.py            # Deriv API error -> McpError mapping
tests/
  conftest.py          # Shared fixtures
  test_connection.py
  test_market.py
  test_account.py
  test_trading.py
pyproject.toml         # Enhanced: ruff, dependency-groups, scripts entry
Dockerfile             # Updated paths
.env.example
README.md
```

Old `server.py` and `services/` directory are replaced entirely.

---

## Connection Manager (`connection.py`)

### Class: DerivAPIManager

Single shared DerivAPI instance across all tools. Initialized via FastMCP lifespan.

**State:**

| Field | Type | Purpose |
|---|---|---|
| `_api` | `DerivAPI \| None` | The WebSocket connection |
| `_authorized` | `bool` | Whether authorize() has been called |
| `_app_id` | `str` | Deriv application ID |
| `_api_token` | `str` | Deriv API token |
| `_tick_buffers` | `dict[str, deque[TickData]]` | Symbol -> bounded deque (maxlen=50) |
| `_active_subscriptions` | `dict[str, SubscriptionState]` | Symbol -> (sub_id, disposable) |
| `_reconnect_task` | `asyncio.Task \| None` | Background reconnection coroutine |
| `_sanity_disposable` | `Disposable \| None` | Cleanup handle for error listener |

**Methods:**

| Method | Purpose |
|---|---|
| `connect()` | Create DerivAPI instance, subscribe to sanity_errors, authorize |
| `disconnect()` | Forget all subscriptions, clear buffers, call api.clear() |
| `ensure_connected()` | Called by every tool. Returns _api if connected, awaits reconnect if in progress, raises McpError if dead |
| `subscribe_ticks(symbol)` | Idempotent. Calls api.subscribe(), wires on_next to deque, stores state for recovery |
| `unsubscribe_ticks(symbol)` | Calls api.forget(sub_id), disposes Observable, clears buffer |
| `unsubscribe_all_ticks()` | Calls api.forget_all('ticks'), clears all tick state |
| `get_tick_buffer(symbol)` | Returns deque contents as list. No API call. |

### Reconnection Flow

1. `sanity_errors.on_next(ConnectionClosed)` fires
2. `_on_connection_error()` marks `_authorized = False`, starts `_reconnect_loop()` as background task
3. `_reconnect_loop()` retries indefinitely with exponential backoff: 1s, 2s, 4s, 8s, 16s, cap at 30s. Only stops when `disconnect()` is explicitly called.
4. On success: `_authorize()`, then `_recover_subscriptions()`
5. `_recover_subscriptions()` iterates `_active_subscriptions`, re-calls `api.subscribe({'ticks': symbol})` for each, re-wires on_next to the existing deque

### Thread Safety

No locks needed. Verified from python-deriv-api 0.1.6 source: `on_next` callbacks fire inside `__wait_data()`, an async task on the event loop. MCP tool handlers also run on the event loop. All deque access is single-threaded.

### Lifespan Integration

```python
@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    manager = DerivAPIManager(app_id=..., api_token=...)
    await manager.connect()
    try:
        yield AppContext(connection=manager)
    finally:
        await manager.disconnect()

mcp = FastMCP("deriv-api-mcp", lifespan=app_lifespan)
```

Tools access the manager via `ctx.request_context.lifespan_context.connection`.

---

## Tools

### Market Data (`tools/market.py`)

**get_active_symbols** (migrated)
- Input: `product_type: str = "basic"`
- Calls `api.active_symbols({"active_symbols": "brief", "product_type": product_type})`
- Returns `list[ActiveSymbol]`
- No auth required

**subscribe_ticks** (new)
- Input: `symbol: str`
- Calls `manager.subscribe_ticks(symbol)`
- Returns confirmation string
- Idempotent

**get_latest_ticks** (new)
- Input: `symbol: str`, `count: int = 10` (1-50)
- Reads from `manager.get_tick_buffer(symbol)` — no API call
- Returns `list[TickData]`
- Returns empty list if not subscribed

**unsubscribe_ticks** (new)
- Input: `symbol: str`
- Calls `manager.unsubscribe_ticks(symbol)`
- Cleans up subscription and buffer

### Account (`tools/account.py`)

**get_account_balance** (migrated)
- Input: none
- Calls `api.balance()`
- Returns `BalanceInfo`

**get_portfolio** (new)
- Input: none
- Calls `api.portfolio({"portfolio": 1})`
- Returns `list[PortfolioContract]`

### Trading (`tools/trading.py`)

**get_proposal** (new)
- Input: `ProposalRequest` (contract_type, symbol, duration, duration_unit, amount, currency, basis)
- Calls `api.proposal({...})`
- Returns `ProposalInfo`

### Tool Pattern

Every tool follows the same structure:
1. Get manager from lifespan context
2. `await manager.ensure_connected()`
3. Wrap in `async with handle_deriv_errors()`
4. Call API method
5. Validate response through Pydantic model
6. Return structured output

---

## Type System (`types.py`)

Pydantic v2 models with `ConfigDict(extra="ignore")` for forward compatibility.

### Input Models

**ProposalRequest:** contract_type (str), symbol (str), duration (int, gt=0), duration_unit (str, "t"/"s"/"m"/"h"/"d"), amount (float, gt=0), currency (str, default="USD"), basis (str, default="stake")

**TickQuery:** symbol (str), count (int, ge=1, le=50, default=10)

### Output Models

**ActiveSymbol:** symbol, display_name, market, market_display_name, pip (float)

**BalanceInfo:** balance (float), currency (str), loginid (str)

**PortfolioContract:** contract_id (int), contract_type (str), symbol (str), buy_price (float), payout (float), expiry_time (int)

**ProposalInfo:** proposal_id (str), ask_price (float), payout (float), spot (float), spot_time (int), display_value (str)

**TickData:** symbol (str), epoch (int), quote (float), ask (float | None), bid (float | None)

---

## Error Handling (`errors.py`)

### Deriv-to-MCP Error Mapping

| Deriv Error Code | MCP Error Code | Category |
|---|---|---|
| AuthorizationRequired | INVALID_REQUEST | Auth |
| InvalidToken | INVALID_REQUEST | Auth |
| InvalidSymbol | INVALID_PARAMS | Validation |
| InvalidContractType | INVALID_PARAMS | Validation |
| RateLimit | INTERNAL_ERROR | Throttling |
| ConnectionClosed | INTERNAL_ERROR | Connection |
| Unknown/other | INTERNAL_ERROR | Catchall |

### Implementation

Async context manager `handle_deriv_errors()` wrapping every tool call. Catches `ResponseError`, `APIError`, and `ConnectionClosed`, maps each to `McpError(ErrorData(...))` per MCP SDK 1.2.0 API.

---

## Project Configuration

### pyproject.toml Changes

- Pin `mcp[cli]>=1.2.0,<2.0`
- Add `[project.scripts]`: `deriv-mcp = "deriv_mcp.server:main"`
- Add `[dependency-groups]`: `dev = ["ruff>=0.9", "pytest>=8.0", "pytest-asyncio>=0.24"]`
- Add `[tool.ruff]`: target-version py311, line-length 100, select ["E", "F", "I", "UP", "B", "SIM", "TCH"]
- Add `[tool.pytest.ini_options]`: asyncio_mode = "auto"

### Dockerfile

- `COPY deriv_mcp/ deriv_mcp/` replaces old COPY lines
- CMD: `python -m deriv_mcp.server`

### README

- List all 7 tools with descriptions
- Configuration: DERIV_APP_ID and DERIV_API_TOKEN in .env
- Claude Desktop integration (uv + Docker methods)
- Remove hardcoded paths

---

## Implementation Sequence

### Phase 1: Scaffolding
1. Create `deriv_mcp/` package structure
2. Enhance `pyproject.toml` (deps, ruff, dependency-groups, scripts)
3. Move existing logic into new locations with zero behavior change
4. Verify it runs: `uv run python -m deriv_mcp.server`

### Phase 2: Connection Manager
5. Implement `DerivAPIManager` — connect/disconnect/ensure_connected/authorize
6. Wire into server.py via lifespan
7. Migrate existing tools to use shared connection
8. Add reconnection logic: sanity_errors subscription, exponential backoff, re-authorization
9. Add subscription recovery

### Phase 3: Types and Errors
10. Define Pydantic models in types.py
11. Implement errors.py with Deriv-to-MCP mapping
12. Retrofit existing tools with validation and error handling

### Phase 4: New Tools
13. Implement get_portfolio in tools/account.py
14. Implement get_proposal in tools/trading.py
15. Implement tick subscription system: subscribe_ticks + get_latest_ticks + unsubscribe_ticks in tools/market.py

### Phase 5: Polish
16. Add tests (connection manager reconnection, error mapping, Pydantic validation)
17. Update Dockerfile
18. Rewrite README
19. Run ruff, fix lint issues
20. Run tsc equivalent: `uv run ty check deriv_mcp/` (if ty added) or verify with ruff type checks

---

## Verification Plan

1. **Structural:** `uv sync --all-groups` succeeds, `uv run ruff check deriv_mcp/` passes, `uv run pytest` passes
2. **Functional — static tools:** Start server with `uv run python -m deriv_mcp.server`, connect via Claude Desktop, call get_active_symbols, get_account_balance, get_portfolio, get_proposal — verify structured JSON responses
3. **Functional — streaming:** Call subscribe_ticks("R_100"), wait 5 seconds, call get_latest_ticks("R_100") — verify tick data arrives in the buffer
4. **Error handling:** Call get_proposal with an invalid symbol — verify clean McpError, not a stack trace
5. **Reconnection:** Manually disconnect WiFi during an active tick subscription, reconnect — verify ticks resume flowing after backoff
6. **Docker:** `docker build -t deriv-api-mcp .` succeeds, `docker run --rm -i deriv-api-mcp` starts without errors
