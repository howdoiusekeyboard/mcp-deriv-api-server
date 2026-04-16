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
