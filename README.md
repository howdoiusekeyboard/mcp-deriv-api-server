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

- **Singleton connection** -- One `DerivAPI` instance multiplexes all requests and subscriptions over a single socket, avoiding redundant TLS handshakes and rate limit pressure.
- **Auto-reconnection** -- On connection drop, exponential backoff (1s to 30s cap) retries indefinitely until the socket is restored.
- **Subscription recovery** -- Active tick subscriptions are automatically re-established after reconnection, keeping the ring buffers populated without manual intervention.
- **Bounded tick buffers** -- Each subscribed symbol stores up to 50 ticks in a `collections.deque`, preventing unbounded memory growth from high-frequency streams.

## Development

```bash
uv sync --all-groups
uv run pytest
uv run ruff check deriv_mcp/
```

## License

MIT
