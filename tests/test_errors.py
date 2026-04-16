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

    async def test_catches_api_error_and_raises_mcp_error(self):
        from deriv_api import APIError

        with pytest.raises(McpError) as exc_info:
            async with handle_deriv_errors():
                raise APIError("Something broke")
        assert exc_info.value.error.code == INTERNAL_ERROR
