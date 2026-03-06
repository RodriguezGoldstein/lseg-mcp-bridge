from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from auth import BridgeError, SessionManager, library_version, redact_value

DEFAULT_ROW_LIMIT = 50
HARD_ROW_CAP = 250
PREVIEW_ROW_LIMIT = 5


def get_live_data(
    universe: str | list[str],
    fields: str | list[str],
    *,
    parameters: dict[str, Any] | str | None = None,
    row_limit: int | None = None,
) -> dict[str, Any]:
    manager = SessionManager.instance()
    session = manager.ensure_session()
    limit = _effective_row_limit(row_limit)

    module = _get_lseg_data()
    try:
        data_frame = module.get_data(universe=universe, fields=fields, parameters=parameters)
    except BridgeError:
        raise
    except Exception as exc:
        raise BridgeError.from_exception(
            "data_request_failed",
            exc,
            details={"source": "ld.get_data"},
        ) from exc

    return _normalize_table_response(
        data_frame,
        source="ld.get_data",
        auth_mode_used=session.auth_mode_used,
        request_metadata={
            "universe": redact_value(universe),
            "fields": redact_value(fields),
            "parameters": redact_value(parameters),
        },
        row_limit=limit,
    )


def get_history_data(
    universe: str | list[str],
    fields: str | list[str] | None,
    *,
    interval: str | None = None,
    start: str | None = None,
    end: str | None = None,
    count: int | None = None,
    parameters: dict[str, Any] | str | None = None,
) -> dict[str, Any]:
    manager = SessionManager.instance()
    session = manager.ensure_session()
    limit = _effective_row_limit(None)

    module = _get_lseg_data()
    try:
        data_frame = module.get_history(
            universe=universe,
            fields=fields,
            interval=interval,
            start=start,
            end=end,
            count=count,
            parameters=parameters,
        )
    except BridgeError:
        raise
    except Exception as exc:
        raise BridgeError.from_exception(
            "data_request_failed",
            exc,
            details={"source": "ld.get_history"},
        ) from exc

    return _normalize_table_response(
        data_frame,
        source="ld.get_history",
        auth_mode_used=session.auth_mode_used,
        request_metadata={
            "universe": redact_value(universe),
            "fields": redact_value(fields),
            "interval": interval,
            "start": start,
            "end": end,
            "count": count,
            "parameters": redact_value(parameters),
        },
        row_limit=limit,
    )


def _normalize_table_response(
    data: Any,
    *,
    source: str,
    auth_mode_used: str,
    request_metadata: dict[str, Any],
    row_limit: int,
) -> dict[str, Any]:
    data_frame = _to_dataframe(data)
    if not isinstance(data_frame.index, pd.RangeIndex):
        data_frame = data_frame.reset_index()

    data_frame.columns = [_normalize_column_name(column) for column in data_frame.columns]
    rows = _dataframe_to_records(data_frame)
    source_row_count = len(rows)
    table_rows = rows[:row_limit]
    preview_rows = table_rows[: min(PREVIEW_ROW_LIMIT, len(table_rows))]

    return {
        "table": {
            "columns": list(data_frame.columns),
            "rows": table_rows,
        },
        "columns": list(data_frame.columns),
        "row_count": source_row_count,
        "preview_rows": preview_rows,
        "execution_metadata": {
            "source": source,
            "auth_mode_used": auth_mode_used,
            "library_version": library_version(),
            "requested": request_metadata,
            "limit_applied": row_limit,
            "hard_cap": HARD_ROW_CAP,
            "returned_row_count": len(table_rows),
            "preview_row_count": len(preview_rows),
            "source_row_count": source_row_count,
            "truncated": source_row_count > len(table_rows),
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        },
    }


def _dataframe_to_records(data_frame: pd.DataFrame) -> list[dict[str, Any]]:
    if data_frame.empty:
        return []
    encoded = data_frame.to_json(orient="records", date_format="iso")
    records = json.loads(encoded)
    return [redact_value(record) for record in records]


def _to_dataframe(data: Any) -> pd.DataFrame:
    if isinstance(data, pd.DataFrame):
        return data.copy()
    if isinstance(data, pd.Series):
        return data.to_frame().reset_index()
    try:
        return pd.DataFrame(data)
    except Exception as exc:
        raise BridgeError.from_exception(
            "data_normalization_failed",
            exc,
            details={"data_type": type(data).__name__},
        ) from exc


def _normalize_column_name(column: Any) -> str:
    if isinstance(column, tuple):
        return ".".join(str(part) for part in column if part not in ("", None))
    return str(column)


def _effective_row_limit(row_limit: int | None) -> int:
    raw_default = os.getenv("LSEG_DEFAULT_ROW_LIMIT", str(DEFAULT_ROW_LIMIT)).strip()
    try:
        resolved_default = int(raw_default)
    except ValueError:
        resolved_default = DEFAULT_ROW_LIMIT

    resolved_default = max(1, min(resolved_default, HARD_ROW_CAP))
    resolved = resolved_default if row_limit is None else row_limit
    if resolved <= 0:
        return resolved_default
    return min(resolved, HARD_ROW_CAP)


def _get_lseg_data() -> Any:
    from auth import get_lseg_module

    return get_lseg_module()
