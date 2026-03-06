from __future__ import annotations

import atexit
import logging
import os
import signal
from typing import Any, Literal

from auth import (
    BridgeError,
    SessionManager,
    available_credential_types,
    library_version,
    normalize_error,
    python_version,
)
from examples import search_examples as search_local_examples
from live_data import get_history_data as fetch_history_data
from live_data import get_live_data as fetch_live_data
from schemas import get_schema as get_schema_details
from validator import validate_code as validate_python_code

try:
    from mcp.server.fastmcp import FastMCP
except ImportError as exc:  # pragma: no cover - depends on runtime dependencies
    raise SystemExit(
        "The 'mcp' package is required to run this server. Install dependencies with "
        "'python -m pip install -r requirements.txt'."
    ) from exc

LOGGER = logging.getLogger("lseg_python_mcp_bridge.server")


def _build_server() -> FastMCP:
    try:
        return FastMCP("lseg-python-mcp-bridge", json_response=True)
    except TypeError:
        return FastMCP("lseg-python-mcp-bridge")


mcp = _build_server()


def _configure_logging() -> None:
    level = logging.DEBUG if os.getenv("DEBUG_MCP", "false").strip().lower() == "true" else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s %(message)s")


def _register_shutdown_hooks() -> None:
    manager = SessionManager.instance()
    atexit.register(manager.close_session)

    def _handle_signal(signum: int, _frame: Any) -> None:
        LOGGER.info("Received signal %s, closing LSEG session.", signum)
        manager.close_session()
        raise SystemExit(0)

    for handled_signal in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(handled_signal, _handle_signal)
        except ValueError:
            continue


def _error_response(defaults: dict[str, Any], exc: Exception, fallback_code: str) -> dict[str, Any]:
    response = dict(defaults)
    response["error"] = normalize_error(exc, fallback_code).to_dict()
    return response


@mcp.tool()
def ping_session(
    auth_mode: Literal["auto", "platform_client_credentials", "platform_password", "desktop"] = "auto",
) -> dict[str, Any]:
    return SessionManager.instance().ping_session(auth_mode=auth_mode)


@mcp.tool()
def list_capabilities() -> dict[str, Any]:
    return {
        "live_data_supported": True,
        "history_supported": True,
        "schema_introspection_supported": True,
        "code_validation_supported": True,
        "example_search_supported": True,
        "supported_auth_modes": [
            "platform_client_credentials",
            "platform_password",
            "desktop",
        ],
        "detected_credential_types": available_credential_types(),
    }


@mcp.tool()
def get_schema(
    path: str,
    include_docstring: bool = True,
    include_members: bool = False,
) -> dict[str, Any]:
    defaults = {
        "import_path": path,
        "object_type": None,
        "signature": None,
        "parameters": [],
        "return_annotation": None,
        "docstring_summary": None,
        "members": [],
    }
    try:
        return get_schema_details(
            path,
            include_docstring=include_docstring,
            include_members=include_members,
        )
    except Exception as exc:
        return _error_response(defaults, exc, "schema_lookup_failed")


@mcp.tool()
def validate_code(
    code: str,
    goal: str | None = None,
    strict: bool = True,
    check_live: bool = False,
) -> dict[str, Any]:
    defaults = {
        "syntax_valid": False,
        "imports_valid": False,
        "detected_lseg_symbols": [],
        "issues": [],
        "suggestions": [],
        "normalized_example_if_possible": None,
    }
    try:
        return validate_python_code(code, goal=goal, strict=strict, check_live=check_live)
    except Exception as exc:
        return _error_response(defaults, exc, "validation_failed")


@mcp.tool()
def get_live_data(
    universe: str | list[str],
    fields: str | list[str],
    parameters: dict[str, Any] | str | None = None,
    row_limit: int | None = None,
) -> dict[str, Any]:
    defaults = {
        "table": {"columns": [], "rows": []},
        "columns": [],
        "row_count": 0,
        "preview_rows": [],
        "execution_metadata": {},
    }
    try:
        return fetch_live_data(universe, fields, parameters=parameters, row_limit=row_limit)
    except Exception as exc:
        return _error_response(defaults, exc, "data_request_failed")


@mcp.tool()
def get_history(
    universe: str | list[str],
    fields: str | list[str] | None,
    interval: str | None = None,
    start: str | None = None,
    end: str | None = None,
    count: int | None = None,
    parameters: dict[str, Any] | str | None = None,
) -> dict[str, Any]:
    defaults = {
        "table": {"columns": [], "rows": []},
        "columns": [],
        "row_count": 0,
        "preview_rows": [],
        "execution_metadata": {},
    }
    try:
        return fetch_history_data(
            universe=universe,
            fields=fields,
            interval=interval,
            start=start,
            end=end,
            count=count,
            parameters=parameters,
        )
    except Exception as exc:
        return _error_response(defaults, exc, "data_request_failed")


@mcp.tool()
def search_examples(
    query: str,
    language: str = "python",
    top_k: int = 5,
    exact_lookup: bool = False,
) -> dict[str, Any]:
    try:
        return search_local_examples(query, language=language, top_k=top_k, exact_lookup=exact_lookup)
    except Exception as exc:
        return _error_response({"matches": []}, exc, "example_search_failed")


@mcp.tool()
def explain_symbol(path: str, context_query: str | None = None) -> dict[str, Any]:
    defaults = {
        "short_explanation": "",
        "common_usage_pattern": "",
        "typical_pitfalls": [],
        "related_symbols": [],
    }
    try:
        schema = get_schema_details(path, include_docstring=True, include_members=True)
        example_note = ""
        if context_query:
            query = " ".join(part for part in (path, context_query) if part).strip()
            matches = search_local_examples(query, top_k=1, exact_lookup=True)["matches"]
            if matches:
                example_note = f" Closest local reference: {matches[0]['title']}."

        return {
            "short_explanation": schema.get("docstring_summary")
            or f"{path} is a {schema.get('object_type', 'symbol')} exposed by the LSEG Python Data Library.",
            "common_usage_pattern": _common_usage_pattern(path, schema) + example_note,
            "typical_pitfalls": _typical_pitfalls(path),
            "related_symbols": _related_symbols(path, schema),
        }
    except Exception as exc:
        return _error_response(defaults, exc, "schema_lookup_failed")


def _common_usage_pattern(path: str, schema: dict[str, Any]) -> str:
    if path == "lseg.data.get_data":
        return "Open an authenticated session, then call ld.get_data(universe=[...], fields=[...], parameters={...})."
    if path == "lseg.data.get_history":
        return "Open an authenticated session, then call ld.get_history(universe='RIC', fields=[...], interval='1d', start='YYYY-MM-DD', end='YYYY-MM-DD')."
    if path == "lseg.data.session.platform.Definition":
        return "Create a platform Definition with app_key plus GrantPassword or ClientCredentials, get the session, set it as default, and open it before data requests."
    signature = schema.get("signature")
    if signature:
        return f"Call {path}{signature} after a session has been authenticated when live data access is required."
    return f"Import and inspect {path} before invoking it in validated code."


def _typical_pitfalls(path: str) -> list[str]:
    pitfalls = [
        "Credentials must come from environment variables only; do not embed secrets in code.",
        "Live requests require an authenticated default session before calling lseg.data quick-access methods.",
    ]
    if path.endswith("get_history"):
        pitfalls.append("Historical requests can return large tables; keep date ranges and counts compact when validating interactively.")
    if path.endswith("get_data"):
        pitfalls.append("Field names and instruments must match the active entitlement set, or the returned DataFrame may contain empty cells or errors.")
    if path.endswith("Definition"):
        pitfalls.append("Choose the grant type that matches the available credential set; client credentials and password grants are not interchangeable.")
    return pitfalls


def _related_symbols(path: str, schema: dict[str, Any]) -> list[str]:
    explicit_map = {
        "lseg.data.get_data": [
            "lseg.data.get_history",
            "lseg.data.session.platform.Definition",
            "lseg.data.open_session",
        ],
        "lseg.data.get_history": [
            "lseg.data.get_data",
            "lseg.data.session.platform.Definition",
            "lseg.data.open_session",
        ],
        "lseg.data.session.platform.Definition": [
            "lseg.data.session.platform.ClientCredentials",
            "lseg.data.session.platform.GrantPassword",
            "lseg.data.open_session",
        ],
    }
    if path in explicit_map:
        return explicit_map[path]

    return [
        member["import_path"]
        for member in schema.get("members", [])[:5]
        if isinstance(member, dict) and "import_path" in member
    ]


def main() -> None:
    _configure_logging()
    _register_shutdown_hooks()
    LOGGER.info(
        "Starting lseg-python-mcp-bridge with Python %s and lseg-data %s.",
        python_version(),
        library_version(),
    )
    mcp.run()


if __name__ == "__main__":
    main()
