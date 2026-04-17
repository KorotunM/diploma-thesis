from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class HealthResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    service: str
    status: Literal["ok"] = "ok"
    environment: str
    version: str
    dependencies: dict[str, str] = Field(default_factory=dict)
