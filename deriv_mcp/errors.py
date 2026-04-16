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
