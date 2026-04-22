from __future__ import annotations

import json
from typing import Any


def sql_text(statement: str) -> Any:
    try:
        from sqlalchemy import text
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "SQLAlchemy is required for normalizer persistence. "
            "Install project runtime dependencies before using normalizer repositories."
        ) from exc
    return text(statement)


def json_to_db(value: dict[str, Any] | None) -> str:
    return json.dumps(value or {}, ensure_ascii=False, sort_keys=True)


def json_from_db(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        decoded = json.loads(value)
        if isinstance(decoded, dict):
            return decoded
    return dict(value)
