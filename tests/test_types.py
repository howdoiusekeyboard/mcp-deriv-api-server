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
