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
