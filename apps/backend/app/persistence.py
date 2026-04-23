from __future__ import annotations

import json
from typing import Any


def sql_text(statement: str) -> Any:
    try:
        from sqlalchemy import text
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "SQLAlchemy is required for backend persistence. "
            "Install project runtime dependencies before using backend repositories."
        ) from exc
    return text(statement)


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
