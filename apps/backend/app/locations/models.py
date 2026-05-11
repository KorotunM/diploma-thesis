from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class LocationSuggestResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[str]
