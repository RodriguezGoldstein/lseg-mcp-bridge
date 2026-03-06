from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

from auth import BridgeError, redact_text

TEXT_EXTENSIONS = {
    ".md",
    ".markdown",
    ".py",
    ".rst",
    ".txt",
    ".json",
    ".yaml",
    ".yml",
    ".ipynb",
}
EXACT_SYMBOL_HINTS = {
    "get_data",
    "get_history",
    "session.platform.definition",
    "lseg.data.get_data",
    "lseg.data.get_history",
    "lseg.data.session.platform.definition",
}


@dataclass(frozen=True, slots=True)
class ExampleDocument:
    path: str
    title: str
    source_type: str
    text: str
    language: str


def search_examples(
    query: str,
    *,
    language: str = "python",
    top_k: int = 5,
    exact_lookup: bool = False,
) -> dict[str, Any]:
    try:
        root = _examples_root()
        use_exact_lookup = exact_lookup or _looks_like_exact_lookup(query)
        matches = _cached_search(
            root,
            query,
            language.lower(),
            max(1, min(top_k, 25)),
            use_exact_lookup,
        )
        return {"matches": matches}
    except BridgeError:
        raise
    except Exception as exc:
        raise BridgeError.from_exception(
            "example_search_failed",
            exc,
            details={"query": query, "language": language},
        ) from exc


def _examples_root() -> str:
    return str(Path(os.getenv("LSEG_EXAMPLES_DIR", "./references/examples")).expanduser().resolve())


@lru_cache(maxsize=256)
def _cached_search(
    root: str,
    query: str,
    language: str,
    top_k: int,
    exact_lookup: bool,
) -> list[dict[str, Any]]:
    normalized_query = query.strip().lower()
    if not normalized_query:
        return []

    tokens = _tokenize(normalized_query)
    variants = _query_variants(normalized_query, tokens)

    scored: list[tuple[int, dict[str, Any]]] = []
    for document in _load_documents(root):
        score, reason = _score_document(
            document=document,
            query=normalized_query,
            tokens=tokens,
            variants=variants,
            language=language,
            exact_lookup=exact_lookup,
        )
        if score <= 0:
            continue
        scored.append(
            (
                score,
                {
                    "source_type": document.source_type,
                    "title": document.title,
                    "path_or_url": document.path,
                    "excerpt": _excerpt(document.text, variants, tokens),
                    "reuse_reason": reason,
                },
            )
        )

    scored.sort(key=lambda item: (-item[0], item[1]["title"], item[1]["path_or_url"]))
    return [match for _, match in scored[:top_k]]


@lru_cache(maxsize=8)
def _load_documents(root: str) -> tuple[ExampleDocument, ...]:
    root_path = Path(root)
    if not root_path.exists():
        return ()

    documents: list[ExampleDocument] = []
    for path in sorted(root_path.rglob("*")):
        if not path.is_file():
            continue
        if path.suffix.lower() not in TEXT_EXTENSIONS:
            continue
        if path.stat().st_size > 2_000_000:
            continue

        text = _read_text(path)
        if not text.strip():
            continue

        documents.append(
            ExampleDocument(
                path=str(path),
                title=_extract_title(path, text),
                source_type=_source_type(path),
                text=text,
                language=_language_for_path(path),
            )
        )
    return tuple(documents)


def _read_text(path: Path) -> str:
    if path.suffix.lower() == ".ipynb":
        try:
            notebook = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return ""
        parts: list[str] = []
        for cell in notebook.get("cells", []):
            source = cell.get("source", [])
            if isinstance(source, list):
                parts.append("".join(source))
            elif isinstance(source, str):
                parts.append(source)
        return "\n".join(parts)

    try:
        return path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""


def _extract_title(path: Path, text: str) -> str:
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("#"):
            return stripped.lstrip("#").strip()
        return stripped[:120]
    return path.stem


def _source_type(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix == ".py":
        return "python_example"
    if suffix == ".ipynb":
        return "notebook"
    if suffix in {".md", ".markdown", ".rst"}:
        return "documentation"
    return "text"


def _language_for_path(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix in {".py", ".ipynb"}:
        return "python"
    return "text"


def _score_document(
    *,
    document: ExampleDocument,
    query: str,
    tokens: list[str],
    variants: list[str],
    language: str,
    exact_lookup: bool,
) -> tuple[int, str]:
    path_lower = document.path.lower()
    filename_lower = Path(document.path).name.lower()
    title_lower = document.title.lower()
    body_lower = document.text.lower()

    score = 0
    reasons: list[str] = []
    exact_hits = 0

    for variant in variants:
        if _contains_exact(path_lower, variant):
            score += 80
            exact_hits += 1
            reasons.append(f"Exact symbol/path hit in the file path: {variant}.")
        if _contains_exact(filename_lower, variant):
            score += 55
            exact_hits += 1
            reasons.append(f"Exact symbol/path hit in the filename: {variant}.")
        if _contains_exact(title_lower, variant):
            score += 65
            exact_hits += 1
            reasons.append(f"Exact symbol/path hit in the title: {variant}.")
        if _contains_exact(body_lower, variant):
            score += 35
            exact_hits += 1
            reasons.append(f"Exact symbol hit appears in the content: {variant}.")

    if exact_lookup and exact_hits == 0:
        return 0, ""

    if query in path_lower:
        score += 30
    if query in title_lower:
        score += 25
    if query in body_lower:
        score += 12

    path_token_hits = sum(1 for token in tokens if token in path_lower)
    title_token_hits = sum(1 for token in tokens if token in title_lower)
    body_token_hits = sum(1 for token in tokens if token in body_lower)

    score += path_token_hits * 10
    score += title_token_hits * 8
    score += body_token_hits * 2

    if language == "python" and document.language == "python":
        score += 12
    if document.source_type == "python_example":
        score += 4
    if any(symbol in body_lower for symbol in ("get_data", "get_history", "session.platform.definition")):
        score += 4

    if score <= 0:
        return 0, ""

    if exact_hits:
        return score, reasons[0]
    if path_token_hits:
        return score, "Query tokens overlap with the path, which usually signals a closer implementation match."
    if title_token_hits:
        return score, "Query tokens overlap with the title, suggesting the document targets the requested topic."
    return score, "Keyword overlap makes this a reasonable local reference."


def _excerpt(text: str, variants: list[str], tokens: list[str], radius: int = 140) -> str:
    normalized = re.sub(r"\s+", " ", text).strip()
    lowered = normalized.lower()

    for candidate in variants + tokens:
        start = lowered.find(candidate)
        if start >= 0:
            excerpt = normalized[max(0, start - radius) : start + radius]
            return redact_text(excerpt.strip())

    return redact_text(normalized[: radius * 2])


def _query_variants(query: str, tokens: list[str]) -> list[str]:
    variants = {query}
    if "." in query:
        variants.add(query.rsplit(".", 1)[-1])
        variants.add(query.replace("lseg.data.", ""))
        variants.add(query.replace(".", " "))
    if "/" in query or "\\" in query:
        variants.add(Path(query).name.lower())
    variants.update(token for token in tokens if token in EXACT_SYMBOL_HINTS)
    variants.update(symbol for symbol in EXACT_SYMBOL_HINTS if query in symbol)
    return [variant for variant in variants if variant]


def _contains_exact(text: str, value: str) -> bool:
    if not value:
        return False
    if any(char in value for char in (".", "/", "\\")):
        return value in text
    return re.search(rf"(?<![a-z0-9_]){re.escape(value)}(?![a-z0-9_])", text) is not None


def _looks_like_exact_lookup(query: str) -> bool:
    normalized = query.strip().lower()
    return (
        normalized in EXACT_SYMBOL_HINTS
        or normalized.startswith("lseg.data.")
        or "/" in normalized
        or "\\" in normalized
    )


def _tokenize(text: str) -> list[str]:
    return [token for token in re.findall(r"[a-z0-9_]{2,}", text.lower()) if token]
