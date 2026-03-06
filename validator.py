from __future__ import annotations

import ast
import importlib
import inspect
import re
from typing import Any

from pydantic import BaseModel

from auth import BridgeError, available_credential_types
from schemas import resolve_import_path

READ_ONLY_RISK_TERMS = {"order", "orders", "update", "delete", "submit", "trade", "modify", "cancel"}
LIVE_VALIDATION_SYMBOLS = {"lseg.data.get_data", "lseg.data.get_history"}
LEGACY_IMPORT_PATTERNS = (
    ("import refinitiv.data as rd", "import lseg.data as ld"),
    ("import refinitiv.data as ld", "import lseg.data as ld"),
    ("from refinitiv.data import", "from lseg.data import"),
)


class ValidationIssue(BaseModel):
    severity: str
    code: str
    message: str
    line: int | None = None
    column: int | None = None
    symbol: str | None = None


def validate_code(
    code: str,
    *,
    goal: str | None = None,
    strict: bool = True,
    check_live: bool = False,
) -> dict[str, Any]:
    try:
        return _validate_code_impl(code, goal=goal, strict=strict, check_live=check_live)
    except BridgeError:
        raise
    except Exception as exc:
        raise BridgeError.from_exception("validation_failed", exc) from exc


def _validate_code_impl(
    code: str,
    *,
    goal: str | None,
    strict: bool,
    check_live: bool,
) -> dict[str, Any]:
    detected_symbols: set[str] = set()
    issues: list[ValidationIssue] = []
    suggestions: list[str] = []

    try:
        tree = ast.parse(code)
    except SyntaxError as exc:
        issues.append(
            ValidationIssue(
                severity="error",
                code="syntax_error",
                message=exc.msg,
                line=exc.lineno,
                column=exc.offset,
            )
        )
        return _result(
            syntax_valid=False,
            imports_valid=False,
            detected_symbols=detected_symbols,
            issues=issues,
            suggestions=suggestions,
            normalized_example=_normalize_example(code),
            live_check_result=_live_result_skipped("Syntax errors prevent safe live inspection.") if check_live else None,
        )

    collector = _SymbolCollector()
    collector.visit(tree)
    issues.extend(collector.issues)
    detected_symbols.update(collector.detected_symbols)

    analyzer = _CallAnalyzer(alias_map=collector.alias_map, strict=strict)
    analyzer.visit(tree)
    issues.extend(analyzer.issues)
    detected_symbols.update(analyzer.detected_symbols)

    if any(symbol.startswith("refinitiv.data") for symbol in detected_symbols):
        suggestions.append("Replace legacy Refinitiv imports with 'lseg.data'.")
    if not any(symbol.startswith(("lseg.data", "refinitiv.data")) for symbol in detected_symbols):
        suggestions.append("No direct lseg.data symbols were detected; confirm the code is targeting the current LSEG package.")
    if goal and "history" in goal.lower() and not any(symbol.endswith("get_history") for symbol in detected_symbols):
        suggestions.append("The goal mentions historical data; consider using lseg.data.get_history().")
    if goal and "live" in goal.lower() and not any(symbol.endswith("get_data") for symbol in detected_symbols):
        suggestions.append("The goal mentions live data; consider using lseg.data.get_data().")
    if not available_credential_types():
        suggestions.append("Live validation is unavailable until one of the supported auth modes is configured in environment variables.")

    live_check_result = _attempt_live_check(tree, collector.alias_map) if check_live else None
    import_errors = [issue for issue in issues if issue.code in {"import_error", "unknown_symbol"}]
    return _result(
        syntax_valid=True,
        imports_valid=not import_errors,
        detected_symbols=detected_symbols,
        issues=issues,
        suggestions=_dedupe_preserving_order(suggestions),
        normalized_example=_normalize_example(code),
        live_check_result=live_check_result,
    )


def _result(
    *,
    syntax_valid: bool,
    imports_valid: bool,
    detected_symbols: set[str],
    issues: list[ValidationIssue],
    suggestions: list[str],
    normalized_example: str | None,
    live_check_result: dict[str, Any] | None,
) -> dict[str, Any]:
    response: dict[str, Any] = {
        "syntax_valid": syntax_valid,
        "imports_valid": imports_valid,
        "detected_lseg_symbols": sorted(detected_symbols),
        "issues": [issue.model_dump(mode="json") for issue in issues],
        "suggestions": suggestions,
        "normalized_example_if_possible": normalized_example,
    }
    if live_check_result is not None:
        response["live_check_result"] = live_check_result
    return response


class _SymbolCollector(ast.NodeVisitor):
    def __init__(self) -> None:
        self.alias_map: dict[str, str] = {}
        self.detected_symbols: set[str] = set()
        self.issues: list[ValidationIssue] = []

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            bound_name = alias.asname or alias.name.split(".", 1)[0]
            self.alias_map[bound_name] = alias.name if alias.asname else alias.name.split(".", 1)[0]

            if alias.name.startswith(("lseg", "refinitiv")):
                self.detected_symbols.add(alias.name)
                self._validate_module(alias.name, node.lineno, node.col_offset)
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        module_name = node.module or ""
        for alias in node.names:
            if alias.name == "*":
                continue
            bound_name = alias.asname or alias.name
            full_name = f"{module_name}.{alias.name}" if module_name else alias.name
            self.alias_map[bound_name] = full_name

            if full_name.startswith(("lseg", "refinitiv")):
                self.detected_symbols.add(full_name)
                self._validate_symbol(full_name, node.lineno, node.col_offset)
        self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign) -> None:
        if len(node.targets) != 1 or not isinstance(node.targets[0], ast.Name):
            self.generic_visit(node)
            return

        resolved = _resolve_expr_path(node.value, self.alias_map)
        if resolved and resolved.startswith(("lseg", "refinitiv")):
            self.alias_map[node.targets[0].id] = resolved
            self.detected_symbols.add(resolved)
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if isinstance(node.target, ast.Name) and node.value is not None:
            resolved = _resolve_expr_path(node.value, self.alias_map)
            if resolved and resolved.startswith(("lseg", "refinitiv")):
                self.alias_map[node.target.id] = resolved
                self.detected_symbols.add(resolved)
        self.generic_visit(node)

    def _validate_module(self, module_name: str, line: int, column: int) -> None:
        try:
            importlib.import_module(module_name)
        except Exception as exc:
            self.issues.append(
                ValidationIssue(
                    severity="error",
                    code="import_error",
                    message=f"Failed to import module '{module_name}': {exc}",
                    line=line,
                    column=column,
                    symbol=module_name,
                )
            )

    def _validate_symbol(self, symbol: str, line: int, column: int) -> None:
        try:
            resolve_import_path(symbol)
        except BridgeError as exc:
            self.issues.append(
                ValidationIssue(
                    severity="error",
                    code="unknown_symbol",
                    message=exc.message,
                    line=line,
                    column=column,
                    symbol=symbol,
                )
            )


class _CallAnalyzer(ast.NodeVisitor):
    def __init__(self, *, alias_map: dict[str, str], strict: bool) -> None:
        self.alias_map = alias_map
        self.strict = strict
        self.detected_symbols: set[str] = set()
        self.issues: list[ValidationIssue] = []

    def visit_Call(self, node: ast.Call) -> None:
        symbol = _resolve_expr_path(node.func, self.alias_map)
        if symbol and symbol.startswith(("lseg", "refinitiv")):
            self.detected_symbols.add(symbol)
            self._check_legacy_symbol(symbol, node)
            self._check_read_only_risks(symbol, node)
            self._check_signature(symbol, node)

        self.generic_visit(node)

    def _check_legacy_symbol(self, symbol: str, node: ast.Call) -> None:
        if symbol.startswith("refinitiv.data"):
            self.issues.append(
                ValidationIssue(
                    severity="warning",
                    code="legacy_package",
                    message="Legacy 'refinitiv.data' usage detected; migrate to 'lseg.data'.",
                    line=node.lineno,
                    column=node.col_offset,
                    symbol=symbol,
                )
            )

    def _check_read_only_risks(self, symbol: str, node: ast.Call) -> None:
        if READ_ONLY_RISK_TERMS.intersection(_path_tokens(symbol)):
            self.issues.append(
                ValidationIssue(
                    severity="warning",
                    code="read_only_policy",
                    message="This bridge is intentionally read-only and should not be used for order or update flows.",
                    line=node.lineno,
                    column=node.col_offset,
                    symbol=symbol,
                )
            )

    def _check_signature(self, symbol: str, node: ast.Call) -> None:
        if symbol.startswith("refinitiv.data"):
            return

        try:
            target = resolve_import_path(symbol)
        except BridgeError as exc:
            self.issues.append(
                ValidationIssue(
                    severity="error",
                    code="unknown_symbol",
                    message=exc.message,
                    line=node.lineno,
                    column=node.col_offset,
                    symbol=symbol,
                )
            )
            return

        try:
            signature = inspect.signature(target)
        except (TypeError, ValueError):
            return

        if any(isinstance(arg, ast.Starred) for arg in node.args) or any(
            keyword.arg is None for keyword in node.keywords
        ):
            return

        positional_args = [object() for _ in node.args]
        keyword_args = {keyword.arg: object() for keyword in node.keywords if keyword.arg}

        try:
            bound = signature.bind_partial(*positional_args, **keyword_args)
        except TypeError as exc:
            self.issues.append(
                ValidationIssue(
                    severity="error",
                    code="signature_mismatch",
                    message=str(exc),
                    line=node.lineno,
                    column=node.col_offset,
                    symbol=symbol,
                )
            )
            return

        if not self.strict:
            return

        missing = [
            parameter.name
            for parameter in signature.parameters.values()
            if parameter.default is inspect.Signature.empty
            and parameter.kind
            not in (
                inspect.Parameter.VAR_POSITIONAL,
                inspect.Parameter.VAR_KEYWORD,
            )
            and parameter.name not in bound.arguments
        ]
        if missing:
            self.issues.append(
                ValidationIssue(
                    severity="warning",
                    code="missing_required_arguments",
                    message=f"Call appears to omit required arguments: {', '.join(missing)}.",
                    line=node.lineno,
                    column=node.col_offset,
                    symbol=symbol,
                )
            )


def _resolve_expr_path(node: ast.AST, alias_map: dict[str, str]) -> str | None:
    if isinstance(node, ast.Name):
        return alias_map.get(node.id)
    if isinstance(node, ast.Attribute):
        base = _resolve_expr_path(node.value, alias_map)
        if base:
            return f"{base}.{node.attr}"
    return None


def _normalize_example(code: str) -> str | None:
    normalized = code
    for before, after in LEGACY_IMPORT_PATTERNS:
        normalized = normalized.replace(before, after)

    normalized = re.sub(r"\brd\.", "ld.", normalized)
    return normalized if normalized != code else None


def _attempt_live_check(tree: ast.AST, alias_map: dict[str, str]) -> dict[str, Any]:
    candidate_errors: list[str] = []

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue

        symbol = _resolve_expr_path(node.func, alias_map)
        if symbol not in LIVE_VALIDATION_SYMBOLS:
            continue

        try:
            call_payload = _literal_call_payload(symbol, node)
        except ValueError as exc:
            candidate_errors.append(f"{symbol}: {exc}")
            continue

        try:
            if symbol.endswith("get_data"):
                from live_data import get_live_data

                response = get_live_data(
                    universe=call_payload["universe"],
                    fields=call_payload["fields"],
                    parameters=call_payload.get("parameters"),
                    row_limit=5,
                )
            else:
                from live_data import get_history_data

                response = get_history_data(
                    universe=call_payload["universe"],
                    fields=call_payload.get("fields"),
                    interval=call_payload.get("interval"),
                    start=call_payload.get("start"),
                    end=call_payload.get("end"),
                    count=call_payload.get("count"),
                    parameters=call_payload.get("parameters"),
                )
            return {
                "attempted": True,
                "success": True,
                "symbol": symbol,
                "row_count": response["row_count"],
                "preview_rows": response["preview_rows"],
            }
        except Exception as exc:
            bridge_error = exc if isinstance(exc, BridgeError) else BridgeError.from_exception("data_request_failed", exc)
            return {
                "attempted": True,
                "success": False,
                "symbol": symbol,
                "error": bridge_error.to_dict(),
            }

    if candidate_errors:
        return _live_result_skipped(
            "Identified live-capable calls but none used safe literal arguments: " + "; ".join(candidate_errors[:2])
        )

    return _live_result_skipped(
        "No confidently resolved lseg.data.get_data() or lseg.data.get_history() call was found for safe live validation."
    )


def _literal_call_payload(symbol: str, call: ast.Call) -> dict[str, Any]:
    target = resolve_import_path(symbol)
    signature = inspect.signature(target)

    positional_values = [_literal_eval(arg, f"positional argument {index + 1}") for index, arg in enumerate(call.args)]
    keyword_values = {
        keyword.arg: _literal_eval(keyword.value, f"keyword argument '{keyword.arg}'")
        for keyword in call.keywords
        if keyword.arg is not None
    }

    if any(keyword.arg is None for keyword in call.keywords):
        raise ValueError("keyword unpacking is not supported for live validation")

    try:
        bound = signature.bind_partial(*positional_values, **keyword_values)
    except TypeError as exc:
        raise ValueError(str(exc)) from exc

    missing = [
        parameter.name
        for parameter in signature.parameters.values()
        if parameter.default is inspect.Signature.empty
        and parameter.kind
        not in (
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        )
        and parameter.name not in bound.arguments
    ]
    if missing:
        raise ValueError(f"missing required literal arguments: {', '.join(missing)}")

    return dict(bound.arguments)


def _literal_eval(node: ast.AST, label: str) -> Any:
    try:
        return ast.literal_eval(node)
    except Exception as exc:
        raise ValueError(f"{label} is not a safe literal: {exc}") from exc


def _path_tokens(symbol: str) -> set[str]:
    return {token for token in re.split(r"[^a-z0-9]+", symbol.lower()) if token}


def _dedupe_preserving_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def _live_result_skipped(reason: str) -> dict[str, Any]:
    return {"attempted": False, "success": False, "reason": reason}
