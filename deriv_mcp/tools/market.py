from __future__ import annotations

from typing import TYPE_CHECKING

from deriv_mcp.errors import handle_deriv_errors
from deriv_mcp.types import ActiveSymbol

if TYPE_CHECKING:
    from mcp.server.fastmcp import Context

    from deriv_mcp.connection import DerivAPIManager


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
