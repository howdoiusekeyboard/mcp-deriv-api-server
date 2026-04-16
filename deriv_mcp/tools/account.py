from __future__ import annotations

from typing import TYPE_CHECKING

from mcp.server.fastmcp import Context  # noqa: TC002

from deriv_mcp.errors import handle_deriv_errors
from deriv_mcp.types import BalanceInfo, PortfolioContract

if TYPE_CHECKING:
    from deriv_mcp.connection import DerivAPIManager


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
