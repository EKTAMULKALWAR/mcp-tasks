"""Task 1 — Customer MCP server (stdio) with strict Pydantic validation.

Run:  python task1_mcp_server/server.py

STDIO isolation (three layers):
  1. All logging is configured on a stderr StreamHandler; there are no print() calls.
  2. The SDK's stdio_server() takes a private dup of the real stdout for the wire and
     points fd 1 at stderr while serving, catching even C-level / subprocess writes.
  3. Once the wire is claimed, sys.stdout is rebound to sys.stderr, so a stray print()
     from this module or any library lands on stderr. (Order matters: rebinding
     before the claim would make the SDK write JSON-RPC to stderr.)

Error mapping (JSON-RPC 2.0):
  -32602 INVALID_PARAMS  schema violation (bad ID format, amount <= 0, short reason,
                         missing/extra fields, wrong types, unknown tool)
  -32603 INTERNAL_ERROR  unexpected server failure (details logged to stderr only)
  Business outcomes (e.g. customer not found) are returned as a tool result with
  isError=true — that's the MCP convention for errors the model should see and act on.
"""

from __future__ import annotations

import json
import logging
import math
import sys
import uuid
from decimal import Decimal
from typing import Any

import anyio
from mcp import types
from mcp.server.lowlevel import Server
from mcp.server.stdio import stdio_server
from mcp.shared.exceptions import MCPError
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

logging.basicConfig(
    stream=sys.stderr,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("customer-mcp")

# ---------------------------------------------------------------- schemas

# [0-9], not \d: \d also matches non-ASCII digits (Arabic-Indic, fullwidth, ...)
CUSTOMER_ID_PATTERN = r"^CUST-[0-9]{5}$"


class _Strict(BaseModel):
    # strict=True: no "12" -> 12 coercion; extra="forbid": unknown keys are rejected.
    model_config = ConfigDict(strict=True, extra="forbid", str_strip_whitespace=False)


class GetCustomerRecordInput(_Strict):
    customer_id: str = Field(pattern=CUSTOMER_ID_PATTERN, description="Format CUST-XXXXX (5 digits)")


class TriggerRefundInput(_Strict):
    customer_id: str = Field(pattern=CUSTOMER_ID_PATTERN, description="Format CUST-XXXXX (5 digits)")
    amount: float = Field(gt=0, le=1_000_000, description="Positive refund amount")
    reason: str = Field(min_length=10, max_length=500, description="At least 10 characters")

    @field_validator("amount", mode="before")
    @classmethod
    def _amount_is_real_number(cls, v: Any) -> Any:
        # JSON has no float/int distinction; accept ints, but never bools, NaN or Infinity.
        if isinstance(v, bool):
            raise ValueError("amount must be a number, not a boolean")
        if isinstance(v, int):
            return float(v)
        if isinstance(v, float) and not math.isfinite(v):
            raise ValueError("amount must be finite")
        return v

    @field_validator("amount")
    @classmethod
    def _max_two_decimals(cls, v: float) -> float:
        exponent = Decimal(str(v)).as_tuple().exponent  # 'n'/'N'/'F' only for NaN/Inf, rejected earlier
        if isinstance(exponent, int) and exponent < -2:
            raise ValueError("amount may have at most 2 decimal places")
        return v

    @field_validator("reason")
    @classmethod
    def _reason_not_blank(cls, v: str) -> str:
        if len(v.strip()) < 10:
            raise ValueError("reason must contain at least 10 non-whitespace-padded characters")
        return v


TOOL_MODELS: dict[str, type[_Strict]] = {
    "get_customer_record": GetCustomerRecordInput,
    "trigger_refund": TriggerRefundInput,
}

# ---------------------------------------------------------------- mock data

CUSTOMERS: dict[str, dict[str, Any]] = {
    "CUST-00001": {"name": "Ada Lovelace", "email": "ada@example.com", "tier": "gold", "balance": 1250.00},
    "CUST-00042": {"name": "Alan Turing", "email": "alan@example.com", "tier": "silver", "balance": 310.55},
}

# ---------------------------------------------------------------- handlers


def _validate(name: str, arguments: dict[str, Any] | None) -> _Strict:
    model = TOOL_MODELS.get(name)
    if model is None:
        raise MCPError(types.INVALID_PARAMS, f"Unknown tool: {name}")
    try:
        return model.model_validate(arguments or {})
    except ValidationError as exc:
        errors = [
            {"field": ".".join(str(p) for p in e["loc"]) or "<root>", "message": e["msg"], "type": e["type"]}
            for e in exc.errors(include_url=False, include_input=False)
        ]
        log.info("rejected %s call: %s", name, errors)
        raise MCPError(types.INVALID_PARAMS, f"Invalid params for {name}", data={"errors": errors})


def _text(payload: str, is_error: bool = False) -> types.CallToolResult:
    return types.CallToolResult(content=[types.TextContent(type="text", text=payload)], is_error=is_error)


async def list_tools(ctx, params) -> types.ListToolsResult:
    return types.ListToolsResult(
        tools=[
            types.Tool(
                name="get_customer_record",
                description="Fetch a customer record by ID (CUST-XXXXX).",
                input_schema=GetCustomerRecordInput.model_json_schema(),
            ),
            types.Tool(
                name="trigger_refund",
                description="Issue a refund to a customer.",
                input_schema=TriggerRefundInput.model_json_schema(),
            ),
        ]
    )


async def call_tool(ctx, params: types.CallToolRequestParams) -> types.CallToolResult:
    args = _validate(params.name, params.arguments)  # raises -32602 before any side effect
    try:
        if isinstance(args, GetCustomerRecordInput):
            record = CUSTOMERS.get(args.customer_id)
            if record is None:
                return _text(f"Customer {args.customer_id} not found", is_error=True)
            return _text(json.dumps({"customer_id": args.customer_id, **record}))

        if isinstance(args, TriggerRefundInput):
            if args.customer_id not in CUSTOMERS:
                return _text(f"Customer {args.customer_id} not found", is_error=True)
            refund_id = f"RF-{uuid.uuid4().hex[:10].upper()}"
            log.info("refund %s: %s %.2f", refund_id, args.customer_id, args.amount)
            return _text(f"Refund {refund_id} of ${args.amount:.2f} queued for {args.customer_id}")
    except MCPError:
        raise
    except Exception:
        log.exception("tool %s failed", params.name)  # stack trace to stderr only
        raise MCPError(types.INTERNAL_ERROR, "Internal error")
    raise MCPError(types.INVALID_PARAMS, f"Unknown tool: {params.name}")


server = Server("customer-mcp", version="1.0.0", on_list_tools=list_tools, on_call_tool=call_tool)


async def main() -> None:
    log.info("starting customer-mcp on stdio")
    async with stdio_server() as (read, write):
        sys.stdout = sys.stderr  # layer 3, after the SDK has claimed the real stdout
        await server.run(read, write, server.create_initialization_options())


if __name__ == "__main__":
    anyio.run(main)
