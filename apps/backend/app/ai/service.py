from __future__ import annotations

import json
import os
import re
from typing import Any

import httpx
from pydantic import ValidationError

from .models import AiChatRequest, AiChatResponse

DEFAULT_MODEL = "gpt-4o-mini"
DEFAULT_BASE_URL = "https://api.openai.com/v1"
DEFAULT_TIMEOUT_SECONDS = 8.0

SYSTEM_PROMPT = """You are an AI assistant for a Russian university search website.
Convert the user's request into search filters and write a short Russian reply.

IMPORTANT — what the search engine ACTUALLY supports:
- filters.query   → full-text search in university names and descriptions (USE THIS for directions, programs, keywords)
- filters.city    → exact city filter (e.g. "Москва", "Санкт-Петербург")
- filters.country → country code (almost always "RU")
The fields budget_type, study_form, dormitory, min_ege_score are extracted for display
but do NOT affect search results (backend limitation). So always put meaningful keywords
into filters.query so the search returns relevant universities.

Query building rules:
- If user mentions a study direction or program → put it in filters.query (e.g. "программирование", "медицина", "IT")
- If user mentions dormitory (общежитие) with no direction → filters.query = null (show all)
- If user mentions budget (бюджет) with a direction → filters.query = direction
- If user mentions only a city → filters.query = null, filters.city = city

You MUST respond with a single JSON object with EXACTLY these fields:
{
  "intent": "search" | "clarify" | "general",
  "message_to_user": "<short reply in Russian — say what was found and note if budget/dormitory can only be checked on each university's page>",
  "filters": {
    "query": null or "<direction or program keywords for text search>",
    "city": null or "<Russian city name>",
    "country": "RU" or null,
    "source_type": null,
    "direction": null or "<study direction>",
    "study_form": null or "full_time" | "part_time" | "mixed" | "distance",
    "budget_type": null or "budget" | "paid",
    "min_ege_score": null or <integer 0-400>,
    "advanced": {
      "min_rating": null,
      "min_budget_places": null,
      "max_passing_score": null,
      "dormitory": null or true | false,
      "university_type": null,
      "program_query": null or "<program keywords>"
    }
  },
  "missing_fields": [],
  "confidence": 0.9
}

Output ONLY the JSON object, no extra text."""

AI_CHAT_RESPONSE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "intent": {"type": "string", "enum": ["search", "clarify", "general"]},
        "message_to_user": {"type": "string"},
        "filters": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "query": {"type": ["string", "null"]},
                "city": {"type": ["string", "null"]},
                "country": {"type": ["string", "null"]},
                "source_type": {"type": ["string", "null"]},
                "direction": {"type": ["string", "null"]},
                "study_form": {
                    "type": ["string", "null"],
                    "enum": ["full_time", "part_time", "mixed", "distance", None],
                },
                "budget_type": {
                    "type": ["string", "null"],
                    "enum": ["budget", "paid", "any", None],
                },
                "min_ege_score": {"type": ["integer", "null"], "minimum": 0, "maximum": 400},
                "advanced": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "min_rating": {"type": ["number", "null"], "minimum": 0},
                        "min_budget_places": {"type": ["integer", "null"], "minimum": 0},
                        "max_passing_score": {"type": ["integer", "null"], "minimum": 0},
                        "dormitory": {"type": ["boolean", "null"]},
                        "university_type": {"type": ["string", "null"]},
                        "program_query": {"type": ["string", "null"]},
                    },
                    "required": [
                        "min_rating",
                        "min_budget_places",
                        "max_passing_score",
                        "dormitory",
                        "university_type",
                        "program_query",
                    ],
                },
            },
            "required": [
                "query",
                "city",
                "country",
                "source_type",
                "direction",
                "study_form",
                "budget_type",
                "min_ege_score",
                "advanced",
            ],
        },
        "missing_fields": {"type": "array", "items": {"type": "string"}},
        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
    },
    "required": ["intent", "message_to_user", "filters", "missing_fields", "confidence"],
}


class AiChatProviderError(RuntimeError):
    pass


class AiChatService:
    def __init__(
        self,
        *,
        api_key: str | None = None,
        base_url: str | None = None,
        model: str | None = None,
        timeout_seconds: float | None = None,
    ) -> None:
        self._api_key = (
            api_key
            or os.getenv("AI_CHAT_API_KEY")
            or os.getenv("OPENAI_API_KEY")
            or os.getenv("OPENROUTER_API_KEY")
        )
        self._base_url = (
            base_url
            or os.getenv("AI_CHAT_BASE_URL")
            or os.getenv("OPENAI_BASE_URL")
            or DEFAULT_BASE_URL
        ).rstrip("/")
        self._model = (
            model or os.getenv("AI_CHAT_MODEL") or os.getenv("OPENAI_MODEL") or DEFAULT_MODEL
        )
        self._timeout_seconds = timeout_seconds or _read_timeout()

    def build_filter_plan(self, request: AiChatRequest) -> AiChatResponse:
        if not self._api_key:
            return self._local_filter_plan(request)
        try:
            return self._provider_filter_plan(request)
        except AiChatProviderError:
            return self._local_filter_plan(request)

    def _provider_filter_plan(self, request: AiChatRequest) -> AiChatResponse:
        payload = self._build_payload(request)
        headers = self._build_headers()
        try:
            with httpx.Client(timeout=self._timeout_seconds) as client:
                response = client.post(
                    f"{self._base_url}/chat/completions",
                    headers=headers,
                    json=payload,
                )
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            detail = _read_provider_error(exc.response)
            raise AiChatProviderError(detail) from exc
        except httpx.HTTPError as exc:
            raise AiChatProviderError("AI provider request failed.") from exc

        provider_payload = response.json()
        # OpenRouter may return 200 with an error body (e.g. rate limit, no compatible model)
        if "error" in provider_payload and "choices" not in provider_payload:
            raise AiChatProviderError(_read_provider_error_body(provider_payload))
        content = _extract_chat_content(provider_payload)
        try:
            result = AiChatResponse.model_validate_json(content)
        except (ValidationError, ValueError) as exc:
            raise AiChatProviderError("AI provider returned invalid filter JSON.") from exc

        model_used = provider_payload.get("model")
        if isinstance(model_used, str):
            result.model_used = model_used
        return result

    def _build_payload(self, request: AiChatRequest) -> dict[str, Any]:
        messages: list[dict[str, str]] = [{"role": "system", "content": SYSTEM_PROMPT}]
        for item in request.history[-8:]:
            messages.append({"role": item.role, "content": item.content[:1200]})
        messages.append({"role": "user", "content": request.message})

        payload: dict[str, Any] = {
            "model": self._model,
            "messages": messages,
            "temperature": 0.1,
            "response_format": {"type": "json_object"},
        }
        return payload

    def _build_headers(self) -> dict[str, str]:
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if "openrouter.ai" in self._base_url:
            site_url = os.getenv("OPENROUTER_SITE_URL")
            app_name = os.getenv("OPENROUTER_APP_NAME") or "Abiturient+"
            if site_url:
                headers["HTTP-Referer"] = site_url
            headers["X-OpenRouter-Title"] = app_name
        return headers

    @staticmethod
    def _local_filter_plan(request: AiChatRequest) -> AiChatResponse:
        message = request.message.strip()
        lowered = message.casefold()
        city = _extract_city(message)
        score = _extract_score(message)
        budget_type = "budget" if "бюджет" in lowered else "paid" if "платн" in lowered else None
        dormitory = True if "общежит" in lowered else None
        study_form = _extract_study_form(lowered)
        direction = _extract_direction(lowered)
        # Use direction as query for text search; skip full message to avoid noise
        query = direction  # None when no direction — shows all universities

        # Build a helpful message based on what was extracted
        parts: list[str] = []
        if direction:
            parts.append(f"направление «{direction}»")
        if city:
            parts.append(f"город {city}")
        if budget_type == "budget":
            parts.append("бюджетные места")
        if dormitory:
            parts.append("наличие общежития")
        note = (
            " Информацию о бюджетных местах и общежитии уточняйте на странице каждого вуза."
            if (budget_type or dormitory) else ""
        )
        if parts:
            message_to_user = f"Подобрал фильтры: {', '.join(parts)}.{note}"
        else:
            message_to_user = f"Показываю все вузы.{note}"

        return AiChatResponse(
            intent="search",
            message_to_user=message_to_user,
            filters={
                "query": query,
                "city": city,
                "country": "RU",
                "source_type": None,
                "direction": direction,
                "study_form": study_form,
                "budget_type": budget_type,
                "min_ege_score": score,
                "advanced": {
                    "min_rating": None,
                    "min_budget_places": None,
                    "max_passing_score": score,
                    "dormitory": dormitory,
                    "university_type": None,
                    "program_query": direction,
                },
            },
            missing_fields=[],
            confidence=0.45,
            model_used="local-fallback",
        )


def _read_timeout() -> float:
    value = os.getenv("AI_CHAT_TIMEOUT_SECONDS")
    if not value:
        return DEFAULT_TIMEOUT_SECONDS
    try:
        timeout = float(value)
    except ValueError:
        return DEFAULT_TIMEOUT_SECONDS
    return timeout if timeout > 0 else DEFAULT_TIMEOUT_SECONDS


def _extract_chat_content(payload: dict[str, Any]) -> str:
    try:
        content = payload["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError) as exc:
        raise AiChatProviderError("AI provider response does not contain a message.") from exc
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        text_parts = [
            item.get("text", "")
            for item in content
            if isinstance(item, dict) and item.get("type") in {None, "text", "output_text"}
        ]
        return "".join(text_parts)
    raise AiChatProviderError("AI provider response has unsupported content format.")


def _read_provider_error(response: httpx.Response) -> str:
    try:
        payload = response.json()
    except json.JSONDecodeError:
        return response.text or "AI provider request failed."
    error = payload.get("error") if isinstance(payload, dict) else None
    if isinstance(error, dict) and isinstance(error.get("message"), str):
        return error["message"]
    if isinstance(payload, dict) and isinstance(payload.get("detail"), str):
        return payload["detail"]
    return "AI provider request failed."


def _read_provider_error_body(payload: dict[str, Any]) -> str:
    error = payload.get("error")
    if isinstance(error, dict) and isinstance(error.get("message"), str):
        return error["message"]
    return "AI provider returned an error."


def _extract_city(message: str) -> str | None:
    match = re.search(r"\b(?:в|городе|г\.)\s+([А-ЯЁA-Z][а-яёa-z-]+)", message)
    if not match:
        return None
    return match.group(1)


def _extract_score(message: str) -> int | None:
    match = re.search(r"(\d{2,3})\s*(?:балл|егэ|eгэ|score)", message.casefold())
    if not match:
        return None
    score = int(match.group(1))
    return score if 0 <= score <= 400 else None


def _extract_study_form(lowered: str) -> str | None:
    if "очно-заоч" in lowered:
        return "mixed"
    if "заоч" in lowered:
        return "part_time"
    if "дистан" in lowered or "онлайн" in lowered:
        return "distance"
    if "очн" in lowered:
        return "full_time"
    return None


def _extract_direction(lowered: str) -> str | None:
    candidates = [
        ("программ", "программирование"),
        ("информат", "информатика"),
        ("it", "IT"),
        ("айти", "IT"),
        ("эконом", "экономика"),
        ("медицин", "медицина"),
        ("юрид", "юриспруденция"),
        ("инженер", "инженерия"),
        ("управлен", "управление"),
    ]
    for marker, direction in candidates:
        if marker in lowered:
            return direction
    return None
