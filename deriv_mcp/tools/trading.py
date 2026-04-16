from __future__ import annotations

from typing import TYPE_CHECKING

from mcp.server.fastmcp import Context  # noqa: TC002

from deriv_mcp.errors import handle_deriv_errors
from deriv_mcp.types import ProposalInfo, ProposalRequest

if TYPE_CHECKING:
    from deriv_mcp.connection import DerivAPIManager


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
