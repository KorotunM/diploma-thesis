from __future__ import annotations

import json
import os
import re
from typing import Any

import httpx
from pydantic import ValidationError

from .models import AiChatRequest, AiChatResponse

# Study-direction groups → program codes. Single source of truth shared by the
# provider and the local fallback so a direction is always applied as program_codes
# (the structured filter) and never dumped into the university-name text query.
DIRECTION_GROUP_CODES: dict[str, list[str]] = {
    "it": [
        "01.03.02", "02.03.02", "02.03.03", "09.03.01", "09.03.02",
        "09.03.03", "09.03.04", "10.03.01",
    ],
    "engineering": [
        "08.03.01", "11.03.01", "11.03.02", "12.03.01", "13.03.01",
        "13.03.02", "15.03.01", "15.03.04", "15.03.06", "27.03.04",
    ],
    "economy": ["38.03.01", "38.03.05", "38.03.06", "38.03.07"],
    "medicine": ["31.05.01", "31.05.02", "31.05.03", "32.05.01", "33.05.01", "34.03.01"],
    "management": ["38.03.02", "38.03.03", "38.03.04", "38.03.05", "27.03.05"],
    "humanities": [
        "37.03.01", "39.03.01", "39.03.02", "40.03.01", "41.03.01", "41.03.05",
        "42.03.01", "42.03.02", "44.03.01", "45.03.01", "45.03.02", "46.03.01",
    ],
}

# Keyword markers (substring match, casefolded) → direction group key.
_DIRECTION_GROUP_MARKERS: tuple[tuple[str, str], ...] = (
    ("it", "it"),
    ("айти", "it"),
    ("программ", "it"),
    ("информат", "it"),
    ("разработ", "it"),
    ("цифров", "it"),
    ("data", "it"),
    ("инженер", "engineering"),
    ("техник", "engineering"),
    ("технолог", "engineering"),
    ("электро", "engineering"),
    ("строит", "engineering"),
    ("эконом", "economy"),
    ("финанс", "economy"),
    ("бухгалт", "economy"),
    ("медиц", "medicine"),
    ("врач", "medicine"),
    ("лечеб", "medicine"),
    ("стоматолог", "medicine"),
    ("управлен", "management"),
    ("менеджм", "management"),
    ("гуманит", "humanities"),
    ("юрид", "humanities"),
    ("педагог", "humanities"),
    ("психолог", "humanities"),
    ("лингвист", "humanities"),
    ("филолог", "humanities"),
    ("истори", "humanities"),
)


_UNIVERSITY_NAME_MARKERS = (
    "универ", "инстит", "академ", "политех", "вуз", "филиал", "колледж",
)


def _looks_like_university_name(text: str) -> bool:
    """Heuristic: an abbreviation (МГУ, ВШЭ) or a name containing вуз keywords."""
    stripped = text.strip()
    lowered = stripped.casefold()
    if any(marker in lowered for marker in _UNIVERSITY_NAME_MARKERS):
        return True
    # Mostly-uppercase short token → an abbreviation like "МГУ"/"ВШЭ"/"МФТИ".
    letters = [ch for ch in stripped if ch.isalpha()]
    if 2 <= len(letters) <= 8 and sum(1 for ch in letters if ch.isupper()) >= 2:
        return True
    return False


def resolve_direction_group(text: str | None) -> str | None:
    if not text:
        return None
    lowered = text.casefold()
    for marker, group in _DIRECTION_GROUP_MARKERS:
        if marker in lowered:
            return group
    return None

DEFAULT_MODEL = "gpt-4o-mini"
DEFAULT_BASE_URL = "https://api.openai.com/v1"
DEFAULT_TIMEOUT_SECONDS = 8.0

EGE_SUBJECTS = (
    "Математика",
    "Русский язык",
    "Физика",
    "Химия",
    "Биология",
    "Информатика",
    "Обществознание",
    "История",
    "Литература",
    "География",
    "Иностранный язык",
)

SYSTEM_PROMPT = f"""You are "Абитуриент+", an expert admissions assistant for a Russian
university search site. You help applicants find universities by translating a free-form
Russian request into the site's search filters. You are precise, proactive, and never invent
universities — you only set filters; the site runs the actual search.

# How the search UI works (this matters!)
The search page has TWO groups of filters:
- VISIBLE filters (always on screen): text query, city/region, "Общежитие" (dormitory) checkbox,
  "Военная кафедра" (military department) checkbox, and sort order.
- HIDDEN/ADVANCED filters (behind an "Баллы ЕГЭ" panel that is collapsed by default):
  EGE subjects and per-subject scores.
When you set a filter, the UI automatically opens the relevant panel and applies it. So decide
from the user's words WHICH filter is the right one — do not dump everything into the text query.

# Filter catalog (only these actually affect results — use them deliberately)
- filters.query → the university-name search box. Put here ONLY a concrete university NAME or
  ABBREVIATION (e.g. "МГУ", "КубГТУ", "Бауманка", "ВШЭ"). NEVER put a study direction, program,
  faculty or generic keyword here (e.g. NOT "программирование", NOT "Гуманитарные науки"). If the
  user did not name a specific university, leave query null.
- filters.direction → a study direction. Use one of these group keys EXACTLY:
  "it", "engineering", "economy", "medicine", "management", "humanities". The site expands a group
  into the right program codes automatically. Set this (not query) whenever the user names a field
  of study. Leave null if no direction.
- filters.city → a Russian city ("Москва", "Казань", ...). Use for cities.
- filters.region → a Russian region/oblast ("Татарстан", "Краснодарский край") when not a city.
- filters.country → "RU" by default.
- filters.dormitory → true when the user wants общежитие/dormitory.
- filters.military_department → true when the user wants военная кафедра/ВУЦ.
- filters.ege_subjects → list of EGE subjects the applicant will take. Canonical names ONLY:
  {", ".join(EGE_SUBJECTS)}.
- filters.ege_scores → object mapping a canonical subject to a score 0-100, when the user gives a
  per-subject score (e.g. "по информатике 85"). A single overall score like "85 баллов" with no
  subject → put it in min_ege_score, not ege_scores.
- filters.sort_by → "rating" | "budget_places" | "avg_passing_score". Pick when the user expresses
  a ranking preference ("самые сильные" → rating, "где больше бюджетных мест" → budget_places,
  "с низким проходным" → avg_passing_score).
- filters.study_form / filters.budget_type / advanced.* are NOT stored on the university card and
  cannot be filtered. Never put a direction into query to compensate — set filters.direction. When
  the user asks for something the card does not track (форма обучения, бюджет/платно, стоимость по
  конкретной программе), still apply the filters you CAN (direction, city, ЕГЭ, общежитие) and say
  plainly in message_to_user that this detail is not in our data and should be checked on the
  university's own page.

# Returning universities
After applying filters the site shows matching universities automatically, and the assistant's
reply is also enriched with the top matches as tappable links — so phrase message_to_user as if you
are presenting concrete universities ("Вот подходящие вузы:"), not just "фильтры применены".

# Conversation rules (be smart, not a parser)
1. READ THE HISTORY. Treat each turn as a refinement of the previous filters, not a fresh start.
   If the user already said "в Москве" earlier and now says "а с общежитием", keep city=Москва and
   add dormitory=true. Never re-ask something already known.
2. CLARIFY WHEN VAGUE. If the request is too underspecified to give a useful result (e.g. just
   "помоги", "хочу учиться", "посоветуй вуз" with no direction/city/criteria), set intent="clarify",
   ask ONE concrete question in message_to_user, and offer 2-4 short, tappable options in
   "suggestions" (e.g. ["IT и программирование", "Медицина", "Экономика", "Гуманитарные науки"]).
   Do not clarify if you already have at least one meaningful filter — just search.
3. SEARCH otherwise: set intent="search", fill the filters, and in message_to_user briefly say what
   you applied. If budget/study-form can't be filtered, mention it can be checked on each card.
4. GENERAL: for questions not about searching (e.g. "что такое бюджетное место?"), set
   intent="general" and answer briefly in message_to_user with empty filters.
5. Always reply in natural Russian, 1-2 sentences. Be warm and concise.

# Output format
Respond with a SINGLE JSON object, no markdown, with EXACTLY these keys:
{{
  "intent": "search" | "clarify" | "general",
  "message_to_user": "<short Russian reply>",
  "filters": {{
    "query": null | "<direction/program keywords>",
    "city": null | "<city>",
    "region": null | "<region>",
    "country": "RU" | null,
    "source_type": null,
    "direction": null | "<study direction>",
    "study_form": null | "full_time" | "part_time" | "mixed" | "distance",
    "budget_type": null | "budget" | "paid",
    "min_ege_score": null | <integer 0-400>,
    "ege_subjects": ["<canonical subject>", ...],
    "ege_scores": {{"<canonical subject>": <0-100>}},
    "dormitory": null | true | false,
    "military_department": null | true | false,
    "sort_by": null | "rating" | "budget_places" | "avg_passing_score",
    "advanced": {{
      "min_rating": null, "min_budget_places": null, "max_passing_score": null,
      "dormitory": null | true | false, "university_type": null,
      "program_query": null | "<program keywords>"
    }}
  }},
  "missing_fields": ["<filter names you still need, for clarify>"],
  "suggestions": ["<short option>", ...],
  "confidence": 0.0-1.0
}}

# Examples
User: "Хочу на программиста в Москве, желательно с общежитием"
→ intent=search, query=null, direction="it", city="Москва", dormitory=true,
  message: "Вот подходящие вузы Москвы по IT-направлениям с общежитием. Наличие мест в общежитии уточняйте на странице вуза."

User: "вшэ"
→ intent=search, query="ВШЭ", direction=null,
  message: "Нашёл вузы по запросу «ВШЭ»."

User: "Гуманитарные науки"
→ intent=search, query=null, direction="humanities",
  message: "Вот вузы по гуманитарным направлениям."

User: "сдаю информатику на 90 и математику на 80"
→ intent=search, ege_subjects=["Информатика","Математика"],
  ege_scores={{"Информатика":90,"Математика":80}},
  message: "Учёл ЕГЭ: информатика 90, математика 80. Открыл панель баллов и подобрал подходящие вузы."

User: "посоветуй вуз"
→ intent=clarify, suggestions=["IT и программирование","Медицина","Экономика","Гуманитарные науки"],
  message: "Чтобы подобрать точнее — какое направление вам интересно?"

Output ONLY the JSON object."""

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
            return self._normalize_plan(self._local_filter_plan(request))
        try:
            return self._normalize_plan(self._provider_filter_plan(request))
        except AiChatProviderError:
            return self._normalize_plan(self._local_filter_plan(request))

    @staticmethod
    def _normalize_plan(response: AiChatResponse) -> AiChatResponse:
        """Enforce the filter contract regardless of what the model returned.

        The university-name box must never hold a study direction, and a direction
        must always be expressed as program codes — so the search applies the right
        (structured) filter instead of a useless free-text query.
        """
        filters = response.filters

        # If the query is actually a direction phrase, move it out of the name box.
        if filters.query:
            group_from_query = resolve_direction_group(filters.query)
            if group_from_query and not _looks_like_university_name(filters.query):
                if not filters.direction:
                    filters.direction = group_from_query
                filters.query = None

        # Normalise the direction to a known group key and expand to program codes.
        group = filters.direction if filters.direction in DIRECTION_GROUP_CODES else (
            resolve_direction_group(filters.direction)
        )
        if group:
            filters.direction = group
            if not filters.program_codes:
                filters.program_codes = list(DIRECTION_GROUP_CODES[group])
        return response

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
        # A rule-based fallback used when no AI provider key is configured. It mirrors the
        # provider behaviour: it reads the whole conversation, applies the right (visible or
        # hidden) filter, and asks a clarifying question when the request is too vague.
        message = request.message.strip()
        lowered = message.casefold()
        history_text = " ".join(item.content for item in request.history).casefold()
        combined = f"{history_text} {lowered}".strip()

        city = _extract_city(message)
        score = _extract_score(message)
        ege_scores = _extract_subject_scores(lowered)
        ege_subjects = sorted(set(_extract_subjects(combined)) | set(ege_scores))
        budget_type = "budget" if "бюджет" in combined else "paid" if "платн" in combined else None
        dormitory = True if "общежит" in combined else None
        military = True if ("военн" in combined or "вуц" in combined) else None
        study_form = _extract_study_form(combined)
        direction = resolve_direction_group(combined)
        sort_by = _extract_sort(combined)
        # The text query is reserved for a concrete university name/abbreviation.
        query = _extract_university_name(message)

        has_signal = any(
            (query, city, dormitory, military, ege_subjects, score, sort_by, budget_type, direction)
        )

        # Nothing actionable → ask a clarifying question with quick options.
        if not has_signal:
            return AiChatResponse(
                intent="clarify",
                message_to_user="Чтобы подобрать точнее — какое направление вам интересно?",
                filters={"country": "RU"},
                missing_fields=["direction"],
                suggestions=[
                    "IT и программирование",
                    "Медицина",
                    "Экономика",
                    "Гуманитарные науки",
                ],
                confidence=0.4,
                model_used="local-fallback",
            )

        parts: list[str] = []
        if query:
            parts.append(f"вуз «{query}»")
        if direction:
            parts.append(f"направление «{_DIRECTION_GROUP_LABELS.get(direction, direction)}»")
        if city:
            parts.append(f"город {city}")
        if ege_subjects:
            parts.append("ЕГЭ: " + ", ".join(s.lower() for s in ege_subjects))
        if dormitory:
            parts.append("общежитие")
        if military:
            parts.append("военная кафедра")
        note = (
            " Форму обучения и бюджет/стоимость наш каталог не хранит — проверьте на странице вуза."
            if (budget_type or study_form) else ""
        )
        message_to_user = (
            f"Вот подходящие вузы — {', '.join(parts)}.{note}"
            if parts
            else f"Показываю вузы.{note}"
        )

        return AiChatResponse(
            intent="search",
            message_to_user=message_to_user,
            filters={
                "query": query,
                "city": city,
                "country": "RU",
                "source_type": None,
                "direction": direction,
                "program_codes": [],
                "study_form": study_form,
                "budget_type": budget_type,
                "min_ege_score": score,
                "ege_subjects": ege_subjects,
                "ege_scores": ege_scores,
                "dormitory": dormitory,
                "military_department": military,
                "sort_by": sort_by,
                "advanced": {
                    "min_rating": None,
                    "min_budget_places": None,
                    "max_passing_score": score,
                    "dormitory": dormitory,
                    "university_type": None,
                    "program_query": None,
                },
            },
            missing_fields=[],
            confidence=0.5,
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


_SUBJECT_MARKERS: tuple[tuple[str, str], ...] = (
    ("информат", "Информатика"),
    ("математ", "Математика"),
    ("русск", "Русский язык"),
    ("физик", "Физика"),
    ("хими", "Химия"),
    ("биолог", "Биология"),
    ("обществ", "Обществознание"),
    ("истори", "История"),
    ("литератур", "Литература"),
    ("географ", "География"),
    ("иностран", "Иностранный язык"),
    ("англ", "Иностранный язык"),
)


def _extract_subjects(lowered: str) -> list[str]:
    found: list[str] = []
    for marker, subject in _SUBJECT_MARKERS:
        if marker in lowered and subject not in found:
            found.append(subject)
    return found


def _extract_subject_scores(lowered: str) -> dict[str, int]:
    """Map per-subject scores like "информатика 85" / "по физике на 70"."""
    scores: dict[str, int] = {}
    for marker, subject in _SUBJECT_MARKERS:
        match = re.search(rf"{marker}[а-яё]*\D{{0,12}}(\d{{2,3}})", lowered)
        if not match:
            continue
        value = int(match.group(1))
        if 0 <= value <= 100:
            scores[subject] = value
    return scores


def _extract_sort(lowered: str) -> str | None:
    if "бюджетн" in lowered and ("больше" in lowered or "много" in lowered):
        return "budget_places"
    if "проходн" in lowered or "низк" in lowered:
        return "avg_passing_score"
    if "сильн" in lowered or "лучш" in lowered or "топ" in lowered or "рейтинг" in lowered:
        return "rating"
    return None


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


_DIRECTION_GROUP_LABELS: dict[str, str] = {
    "it": "IT и программирование",
    "engineering": "Инженерия",
    "economy": "Экономика",
    "medicine": "Медицина",
    "management": "Управление",
    "humanities": "Гуманитарные науки",
}


def _extract_university_name(message: str) -> str | None:
    """Return a university name/abbreviation if the message clearly names one."""
    stripped = message.strip()
    # Whole short message that looks like an abbreviation/name (e.g. "вшэ", "МГУ").
    if len(stripped) <= 24 and _looks_like_university_name(stripped):
        return stripped
    for token in re.findall(r"[A-Za-zА-Яа-яЁё][A-Za-zА-Яа-яЁё.-]{1,15}", stripped):
        if _looks_like_university_name(token):
            return token
    return None
