from __future__ import annotations

import importlib
import inspect
from functools import lru_cache
from typing import Any

from pydantic import BaseModel, Field

from auth import BridgeError, get_lseg_module, redact_value


class ParameterSchema(BaseModel):
    name: str
    kind: str
    required: bool
    default: Any | None = None
    annotation: str | None = None


class MemberSchema(BaseModel):
    name: str
    import_path: str
    object_type: str
    signature: str | None = None
    docstring_summary: str | None = None


class ObjectSchema(BaseModel):
    import_path: str
    object_type: str
    signature: str | None = None
    parameters: list[ParameterSchema] = Field(default_factory=list)
    return_annotation: str | None = None
    docstring_summary: str | None = None
    members: list[MemberSchema] = Field(default_factory=list)


def get_schema(
    path: str,
    *,
    include_docstring: bool = True,
    include_members: bool = False,
) -> dict[str, Any]:
    try:
        get_lseg_module()
        result = _cached_schema(path, include_docstring, include_members)
        return result.model_dump(mode="json")
    except BridgeError:
        raise
    except Exception as exc:
        raise BridgeError.from_exception(
            "schema_lookup_failed",
            exc,
            details={"path": path},
        ) from exc


@lru_cache(maxsize=256)
def _cached_schema(path: str, include_docstring: bool, include_members: bool) -> ObjectSchema:
    obj = resolve_import_path(path)
    signature = _safe_signature(obj)
    parameters: list[ParameterSchema] = []
    return_annotation: str | None = None

    if signature is not None:
        return_annotation = _format_annotation(signature.return_annotation)
        parameters = [
            ParameterSchema(
                name=parameter.name,
                kind=parameter.kind.name,
                required=parameter.default is inspect.Signature.empty
                and parameter.kind
                not in (
                    inspect.Parameter.VAR_POSITIONAL,
                    inspect.Parameter.VAR_KEYWORD,
                ),
                default=_normalize_signature_value(parameter.default),
                annotation=_format_annotation(parameter.annotation),
            )
            for parameter in signature.parameters.values()
        ]

    docstring_summary = _docstring_summary(obj) if include_docstring else None
    members = _member_summaries(path, obj) if include_members else []

    return ObjectSchema(
        import_path=path,
        object_type=_object_type(obj),
        signature=str(signature) if signature is not None else None,
        parameters=parameters,
        return_annotation=return_annotation,
        docstring_summary=docstring_summary,
        members=members,
    )


@lru_cache(maxsize=256)
def resolve_import_path(path: str) -> Any:
    if not path or not isinstance(path, str):
        raise BridgeError(
            code="schema_lookup_failed",
            message="The schema import path must be a non-empty string.",
            details={"path": path, "reason": "invalid_import_path"},
        )

    parts = path.split(".")
    import_error: Exception | None = None
    for index in range(len(parts), 0, -1):
        module_name = ".".join(parts[:index])
        try:
            obj: Any = importlib.import_module(module_name)
        except Exception as exc:
            import_error = exc
            continue

        for attribute in parts[index:]:
            if not hasattr(obj, attribute):
                raise BridgeError(
                    code="schema_lookup_failed",
                    message=f"Import path '{path}' could not be resolved.",
                    details={
                        "path": path,
                        "reason": "missing_attribute",
                        "missing_attribute": attribute,
                    },
                )
            obj = getattr(obj, attribute)
        return obj

    raise BridgeError.from_exception(
        "schema_lookup_failed",
        import_error or ImportError(path),
        details={"path": path, "reason": "module_import_failed"},
    )


def _docstring_summary(obj: Any) -> str | None:
    docstring = inspect.getdoc(obj)
    if not docstring:
        return None
    first_block = docstring.split("\n\n", 1)[0].strip()
    return first_block if first_block else None


def _member_summaries(path: str, obj: Any, limit: int = 25) -> list[MemberSchema]:
    if not inspect.ismodule(obj) and not inspect.isclass(obj):
        return []

    members: list[MemberSchema] = []
    for member_name in sorted(dir(obj)):
        if member_name.startswith("_"):
            continue
        try:
            member = getattr(obj, member_name)
        except Exception:
            continue

        if not (inspect.isclass(member) or inspect.isroutine(member) or callable(member)):
            continue

        members.append(
            MemberSchema(
                name=member_name,
                import_path=f"{path}.{member_name}",
                object_type=_object_type(member),
                signature=_signature_string(member),
                docstring_summary=_docstring_summary(member),
            )
        )
        if len(members) >= limit:
            break
    return members


def _safe_signature(obj: Any) -> inspect.Signature | None:
    try:
        return inspect.signature(obj)
    except (TypeError, ValueError):
        return None


def _signature_string(obj: Any) -> str | None:
    signature = _safe_signature(obj)
    return str(signature) if signature is not None else None


def _object_type(obj: Any) -> str:
    if inspect.ismodule(obj):
        return "module"
    if inspect.isclass(obj):
        return "class"
    if inspect.ismethod(obj):
        return "method"
    if inspect.isfunction(obj) or inspect.isbuiltin(obj):
        return "function"
    if callable(obj):
        return "callable"
    return type(obj).__name__


def _format_annotation(annotation: Any) -> str | None:
    if annotation is inspect.Signature.empty:
        return None
    if isinstance(annotation, str):
        return annotation
    if getattr(annotation, "__module__", None) == "builtins":
        return getattr(annotation, "__qualname__", repr(annotation))
    representation = repr(annotation)
    return representation.replace("typing.", "")


def _normalize_signature_value(value: Any) -> Any | None:
    if value is inspect.Signature.empty:
        return None
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return redact_value(repr(value))
