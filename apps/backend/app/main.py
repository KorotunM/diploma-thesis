from uuid import UUID

from fastapi import Depends, HTTPException, Query, Request, status

from apps.backend.app.admin_review import (
    ReviewCaseItem,
    ReviewCaseListResponse,
    ReviewCaseNotFoundError,
    ReviewCaseResolveRequest,
    ReviewCaseService,
)
from apps.backend.app.admin_pipeline import (
    AdminPipelineService,
    PipelineRerunRequest,
    PipelineRerunResponse,
    PipelineRunsResponse,
    PipelineSourcesResponse,
    SchedulerAdminError,
)
from apps.backend.app.ai import AiChatProviderError, AiChatRequest, AiChatResponse, AiChatService
from apps.backend.app.ai.usage import AiChatLimitExceededError, AiChatUsageRepository
from apps.backend.app.auth import (
    AuthResponse,
    AuthService,
    CurrentUserResponse,
    EmailAlreadyTakenError,
    InvalidCredentialsError,
    LoginRequest,
    RegisterRequest,
)
from apps.backend.app.cards import (
    UniversityCardNotFoundError,
    UniversityCardReadService,
    UniversityCardResponse,
)
from apps.backend.app.dependencies import (
    get_admin_pipeline_service,
    get_ai_chat_service,
    get_auth_service,
    get_bearer_token,
    get_ege_subject_repository,
    get_location_suggest_service,
    get_optional_user_id,
    get_program_directory_service,
    get_rankings_service,
    get_required_user_id,
    get_review_case_service,
    get_university_card_read_service,
    get_university_provenance_read_service,
    get_university_search_service,
    get_user_service,
)
from apps.backend.app.locations import LocationSuggestResponse, LocationSuggestService
from apps.backend.app.programs import ProgramDirectoryResponse, ProgramDirectoryService
from apps.backend.app.provenance import (
    UniversityProvenanceNotFoundError,
    UniversityProvenanceReadService,
    UniversityProvenanceTrace,
)
from apps.backend.app.rankings import RankingsResponse, RankingsService
from apps.backend.app.search import UniversitySearchResponse, UniversitySearchService
from apps.backend.app.subjects import EgeSubjectRepository
from apps.backend.app.user import (
    ComparisonResponse,
    FavoritesResponse,
    SavedSearchCreateRequest,
    SavedSearchesResponse,
    SavedSearchItem,
    UserRepository,
    UserService,
)
from libs.observability import create_service_app
from libs.storage import get_postgres_session_factory

app = create_service_app(
    service_name="backend",
    description="Serves delivery projections and provenance traces to the UI.",
)

LOCATION_SUGGEST_SERVICE_DEPENDENCY = Depends(get_location_suggest_service)
RANKINGS_SERVICE_DEPENDENCY = Depends(get_rankings_service)
CARD_READ_SERVICE_DEPENDENCY = Depends(get_university_card_read_service)
PROVENANCE_READ_SERVICE_DEPENDENCY = Depends(get_university_provenance_read_service)
SEARCH_SERVICE_DEPENDENCY = Depends(get_university_search_service)
PROGRAM_DIRECTORY_SERVICE_DEPENDENCY = Depends(get_program_directory_service)
REVIEW_CASE_SERVICE_DEPENDENCY = Depends(get_review_case_service)
AUTH_SERVICE_DEPENDENCY = Depends(get_auth_service)
AI_CHAT_SERVICE_DEPENDENCY = Depends(get_ai_chat_service)
ADMIN_PIPELINE_SERVICE_DEPENDENCY = Depends(get_admin_pipeline_service)
USER_SERVICE_DEPENDENCY = Depends(get_user_service)
OPTIONAL_USER_ID_DEPENDENCY = Depends(get_optional_user_id)
REQUIRED_USER_ID_DEPENDENCY = Depends(get_required_user_id)


@app.get("/", tags=["backend"])
def backend_overview() -> dict[str, object]:
    return {
        "service": "backend",
        "public_endpoints": [
            "/api/v1/search",
            "/api/v1/ai/chat",
            "/api/v1/universities/{university_id}",
            "/api/v1/auth/register",
            "/api/v1/auth/login",
        ],
    }


# ── Search ─────────────────────────────────────────────────────────────────────

EGE_SUBJECTS_REPOSITORY_DEPENDENCY = Depends(get_ege_subject_repository)
EGE_SUBJECTS_QUERY = Query(default=None)
EGE_SCORES_QUERY = Query(default=None)
PROGRAM_CODES_QUERY = Query(default=None)


@app.get("/api/v1/subjects", tags=["search"])
def get_ege_subjects(
    repo: EgeSubjectRepository = EGE_SUBJECTS_REPOSITORY_DEPENDENCY,
) -> dict:
    subjects = repo.list_all()
    return {"subjects": [{"id": s.code, "label": s.label} for s in subjects]}


@app.get("/api/v1/programs", response_model=ProgramDirectoryResponse, tags=["programs"])
def list_programs(
    service: ProgramDirectoryService = PROGRAM_DIRECTORY_SERVICE_DEPENDENCY,
) -> ProgramDirectoryResponse:
    return service.list_programs()


@app.get("/api/v1/search", response_model=UniversitySearchResponse, tags=["search"])
def search_universities(
    query: str = "",
    city: str | None = None,
    region: str | None = None,
    country: str | None = None,
    source_type: str | None = None,
    ege_subjects: list[str] | None = EGE_SUBJECTS_QUERY,
    ege_scores: list[str] | None = EGE_SCORES_QUERY,
    program_codes: list[str] | None = PROGRAM_CODES_QUERY,
    dormitory: bool = False,
    military_department: bool = False,
    sort_by: str = "rating",
    page: int = 1,
    page_size: int = 20,
    service: UniversitySearchService = SEARCH_SERVICE_DEPENDENCY,
) -> UniversitySearchResponse:
    search_kwargs = {
        "city": city,
        "region": region,
        "country": country,
        "source_type": source_type,
        "ege_subjects": ege_subjects,
        "program_codes": program_codes,
        "dormitory": dormitory,
        "military_department": military_department,
        "sort_by": sort_by,
        "page": page,
        "page_size": page_size,
    }
    parsed_ege_scores = _parse_ege_scores(ege_scores)
    if parsed_ege_scores:
        search_kwargs["ege_scores"] = parsed_ege_scores
    return service.search(query, **search_kwargs)


def _parse_ege_scores(values: list[str] | None) -> dict[str, int]:
    result: dict[str, int] = {}
    for value in values or []:
        if ":" not in value:
            continue
        subject, score_text = value.split(":", 1)
        subject = subject.strip()
        if not subject:
            continue
        try:
            score = int(score_text)
        except ValueError:
            continue
        result[subject] = score
    return result


@app.get("/api/v1/regions", response_model=LocationSuggestResponse, tags=["locations"])
def suggest_regions(
    q: str = "",
    service: LocationSuggestService = LOCATION_SUGGEST_SERVICE_DEPENDENCY,
) -> LocationSuggestResponse:
    return service.suggest_regions(q)


@app.get("/api/v1/cities", response_model=LocationSuggestResponse, tags=["locations"])
def suggest_cities(
    q: str = "",
    region: str = "",
    service: LocationSuggestService = LOCATION_SUGGEST_SERVICE_DEPENDENCY,
) -> LocationSuggestResponse:
    if region and not q:
        return service.suggest_cities_by_region(region)
    return service.suggest_cities(q)


# ── Rankings ───────────────────────────────────────────────────────────────────

@app.get("/api/v1/rankings", response_model=RankingsResponse, tags=["rankings"])
def get_rankings(
    page: int = 1,
    page_size: int = 20,
    service: RankingsService = RANKINGS_SERVICE_DEPENDENCY,
) -> RankingsResponse:
    return service.get_rankings(page=page, page_size=page_size)


# ── Admin review inbox ─────────────────────────────────────────────────────────

@app.get("/api/v1/admin/review-cases", response_model=ReviewCaseListResponse, tags=["admin"])
def list_review_cases(
    status: str | None = "open",
    limit: int = 50,
    offset: int = 0,
    service: ReviewCaseService = REVIEW_CASE_SERVICE_DEPENDENCY,
    user_id: UUID = REQUIRED_USER_ID_DEPENDENCY,
) -> ReviewCaseListResponse:
    _ = user_id
    return service.list_cases(status=status, limit=limit, offset=offset)


@app.post(
    "/api/v1/admin/review-cases/{review_case_id}/resolve",
    response_model=ReviewCaseItem,
    tags=["admin"],
)
def resolve_review_case(
    review_case_id: UUID,
    body: ReviewCaseResolveRequest,
    service: ReviewCaseService = REVIEW_CASE_SERVICE_DEPENDENCY,
    user_id: UUID = REQUIRED_USER_ID_DEPENDENCY,
) -> ReviewCaseItem:
    try:
        return service.resolve_case(review_case_id, user_id=user_id, body=body)
    except ReviewCaseNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Review case {review_case_id} was not found.",
        ) from exc


# ── Admin pipeline control ───────────────────────────────────────────────────

@app.get(
    "/api/v1/admin/pipeline/sources",
    response_model=PipelineSourcesResponse,
    tags=["admin"],
)
def list_pipeline_sources(
    service: AdminPipelineService = ADMIN_PIPELINE_SERVICE_DEPENDENCY,
    user_id: UUID = REQUIRED_USER_ID_DEPENDENCY,
) -> PipelineSourcesResponse:
    _ = user_id
    try:
        return service.list_sources()
    except SchedulerAdminError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@app.get(
    "/api/v1/admin/pipeline/runs",
    response_model=PipelineRunsResponse,
    tags=["admin"],
)
def list_pipeline_runs(
    limit: int = 50,
    service: AdminPipelineService = ADMIN_PIPELINE_SERVICE_DEPENDENCY,
    user_id: UUID = REQUIRED_USER_ID_DEPENDENCY,
) -> PipelineRunsResponse:
    _ = user_id
    try:
        return service.list_runs(limit=limit)
    except SchedulerAdminError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@app.post(
    "/api/v1/admin/pipeline/rerun",
    response_model=PipelineRerunResponse,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["admin"],
)
def rerun_pipeline(
    body: PipelineRerunRequest,
    service: AdminPipelineService = ADMIN_PIPELINE_SERVICE_DEPENDENCY,
    user_id: UUID = REQUIRED_USER_ID_DEPENDENCY,
) -> PipelineRerunResponse:
    _ = user_id
    try:
        return service.rerun(body)
    except SchedulerAdminError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


# ── University card ────────────────────────────────────────────────────────────

# --- AI chat ---------------------------------------------------------------

@app.post("/api/v1/ai/chat", response_model=AiChatResponse, tags=["ai"])
def ai_chat(
    body: AiChatRequest,
    request: Request,
    service: AiChatService = AI_CHAT_SERVICE_DEPENDENCY,
    search_service: UniversitySearchService = SEARCH_SERVICE_DEPENDENCY,
    user_id: UUID | None = OPTIONAL_USER_ID_DEPENDENCY,
) -> AiChatResponse:
    try:
        try:
            usage_remaining = _record_ai_chat_usage(
                user_id=user_id,
                client_id=body.client_id or _request_client_id(request),
            )
        except ModuleNotFoundError:
            usage_remaining = None
        response = service.build_filter_plan(body)
        response.trial_remaining = usage_remaining
        if response.intent == "search":
            response.universities = _ai_chat_universities(search_service, response)
        return response
    except AiChatLimitExceededError as exc:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=str(exc),
        ) from exc
    except AiChatProviderError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=str(exc),
        ) from exc


def _ai_chat_universities(
    search_service: UniversitySearchService,
    response: AiChatResponse,
) -> list:
    """Run the planned search and return a few concrete universities for the chat.

    Best-effort: a search failure must never break the assistant reply.
    """
    from apps.backend.app.ai.models import AiChatUniversity

    filters = response.filters
    try:
        result = search_service.search(
            filters.query or "",
            city=filters.city,
            region=filters.region,
            country=filters.country,
            program_codes=filters.program_codes or None,
            ege_subjects=filters.ege_subjects or None,
            dormitory=bool(filters.dormitory),
            military_department=bool(filters.military_department),
            sort_by=filters.sort_by or "rating",
            page=1,
            page_size=4,
        )
    except Exception:  # noqa: BLE001 - chat must survive a search failure
        return []
    return [
        AiChatUniversity(
            university_id=str(item.university_id),
            name=_university_short_name(item.canonical_name, item.aliases),
            full_name=item.canonical_name,
            city=item.city,
            score=item.score,
        )
        for item in result.items
    ]


_ACRONYM_STOPWORDS = {"и", "имени", "им", "на", "по", "в", "при"}


def _is_acronymish(text: str) -> bool:
    """Short, uppercase-dominated token like "МФТИ", "НИУ ВШЭ" (spaces allowed)."""
    letters = [ch for ch in text if ch.isalpha()]
    if len(letters) < 2 or len(text) > 16:
        return False
    upper = sum(1 for ch in letters if ch.isupper())
    return upper / len(letters) >= 0.7


def _has_cyrillic(text: str) -> bool:
    return any("Ѐ" <= ch <= "ӿ" for ch in text)


def _acronym_from_name(canonical_name: str) -> str:
    letters: list[str] = []
    for raw_word in canonical_name.split():
        word = raw_word.strip(".,()«»\"'")
        if not word or not word[0].isalpha():
            continue
        if word.casefold() in _ACRONYM_STOPWORDS:
            break  # everything after "имени"/"при" is a qualifier — stop
        letters.append(word[0].upper())
    return "".join(letters)


def _university_short_name(canonical_name: str, aliases: list[str]) -> str:
    """Compact label for the chat: a known Russian abbreviation if available.

    Full names overflow the narrow chat bubble, so we show e.g. "ДГТУ"/"НИУ ВШЭ"
    instead of the full official title. We prefer a Cyrillic abbreviation alias,
    then a generated Cyrillic acronym, and only then a Latin abbreviation.
    """
    candidates = [alias.strip() for alias in aliases if alias and alias.strip()]
    abbreviations = [alias for alias in candidates if _is_acronymish(alias)]

    cyrillic = [alias for alias in abbreviations if _has_cyrillic(alias)]
    if cyrillic:
        return min(cyrillic, key=len)

    acronym = _acronym_from_name(canonical_name)
    if 2 <= len(acronym) <= 7:
        return acronym

    if abbreviations:
        return min(abbreviations, key=len)
    return canonical_name if len(canonical_name) <= 28 else f"{canonical_name[:27]}…"


def _request_client_id(request: Request) -> str:
    if request.client is None:
        return "anonymous"
    return request.client.host or "anonymous"


def _record_ai_chat_usage(*, user_id: UUID | None, client_id: str) -> int | None:
    session_factory = get_postgres_session_factory(service_name="backend")
    session = session_factory()
    try:
        usage = AiChatUsageRepository(session).record_request(user_id=user_id, client_id=client_id)
        return usage.remaining
    finally:
        session.close()


@app.get(
    "/api/v1/universities/{university_id}",
    response_model=UniversityCardResponse,
    tags=["universities"],
)
def get_university_card(
    university_id: UUID,
    service: UniversityCardReadService = CARD_READ_SERVICE_DEPENDENCY,
    user_id: UUID | None = OPTIONAL_USER_ID_DEPENDENCY,
) -> UniversityCardResponse:
    try:
        card = service.get_latest_card(university_id)
    except UniversityCardNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"University card {university_id} was not found.",
        ) from exc

    if user_id is not None:
        session_factory = get_postgres_session_factory(service_name="backend")
        session = session_factory()
        try:
            user_service = UserService(UserRepository(session))
            card.is_favorite = user_service.is_favorite(user_id, university_id)
            card.is_compared = user_service.is_compared(user_id, university_id)
        finally:
            session.close()

    return card


@app.get(
    "/api/v1/universities/{university_id}/provenance",
    response_model=UniversityProvenanceTrace,
    tags=["universities"],
)
def get_university_provenance(
    university_id: UUID,
    service: UniversityProvenanceReadService = PROVENANCE_READ_SERVICE_DEPENDENCY,
) -> UniversityProvenanceTrace:
    try:
        return service.get_latest_trace(university_id)
    except UniversityProvenanceNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"University provenance {university_id} was not found.",
        ) from exc


# ── Auth ───────────────────────────────────────────────────────────────────────

@app.post("/api/v1/auth/register", response_model=AuthResponse, status_code=201, tags=["auth"])
def register(
    body: RegisterRequest,
    service: AuthService = AUTH_SERVICE_DEPENDENCY,
) -> AuthResponse:
    try:
        return service.register(
            email=body.email,
            password=body.password,
            display_name=body.display_name,
        )
    except EmailAlreadyTakenError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc


@app.post("/api/v1/auth/login", response_model=AuthResponse, tags=["auth"])
def login(
    body: LoginRequest,
    service: AuthService = AUTH_SERVICE_DEPENDENCY,
) -> AuthResponse:
    try:
        return service.login(email=body.email, password=body.password)
    except InvalidCredentialsError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password.",
        ) from exc


@app.post("/api/v1/auth/logout", status_code=204, tags=["auth"])
def logout(
    token: str | None = Depends(get_bearer_token),
    service: AuthService = AUTH_SERVICE_DEPENDENCY,
) -> None:
    if token:
        service.logout(token)


@app.get("/api/v1/auth/me", response_model=CurrentUserResponse, tags=["auth"])
def get_me(
    token: str | None = Depends(get_bearer_token),
    service: AuthService = AUTH_SERVICE_DEPENDENCY,
) -> CurrentUserResponse:
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated.")
    try:
        return service.get_current_user(token)
    except InvalidCredentialsError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired token."
        ) from exc


# ── Favorites ──────────────────────────────────────────────────────────────────

@app.get("/api/v1/me/favorites", response_model=FavoritesResponse, tags=["user"])
def get_favorites(
    user_id: UUID = REQUIRED_USER_ID_DEPENDENCY,
    service: UserService = USER_SERVICE_DEPENDENCY,
) -> FavoritesResponse:
    return service.get_favorites(user_id)


@app.post("/api/v1/me/favorites/{university_id}", status_code=201, tags=["user"])
def add_favorite(
    university_id: UUID,
    user_id: UUID = REQUIRED_USER_ID_DEPENDENCY,
    service: UserService = USER_SERVICE_DEPENDENCY,
) -> dict:
    service.add_favorite(user_id, university_id)
    return {"status": "added"}


@app.delete("/api/v1/me/favorites/{university_id}", status_code=204, tags=["user"])
def remove_favorite(
    university_id: UUID,
    user_id: UUID = REQUIRED_USER_ID_DEPENDENCY,
    service: UserService = USER_SERVICE_DEPENDENCY,
) -> None:
    service.remove_favorite(user_id, university_id)


# ── Comparisons ────────────────────────────────────────────────────────────────

@app.get("/api/v1/me/comparisons", response_model=ComparisonResponse, tags=["user"])
def get_comparisons(
    user_id: UUID = REQUIRED_USER_ID_DEPENDENCY,
    service: UserService = USER_SERVICE_DEPENDENCY,
) -> ComparisonResponse:
    return service.get_comparisons(user_id)


@app.post("/api/v1/me/comparisons/{university_id}", status_code=201, tags=["user"])
def add_comparison(
    university_id: UUID,
    user_id: UUID = REQUIRED_USER_ID_DEPENDENCY,
    service: UserService = USER_SERVICE_DEPENDENCY,
) -> dict:
    service.add_comparison(user_id, university_id)
    return {"status": "added"}


@app.delete("/api/v1/me/comparisons/{university_id}", status_code=204, tags=["user"])
def remove_comparison(
    university_id: UUID,
    user_id: UUID = REQUIRED_USER_ID_DEPENDENCY,
    service: UserService = USER_SERVICE_DEPENDENCY,
) -> None:
    service.remove_comparison(user_id, university_id)


# --- Saved searches ---------------------------------------------------------

@app.get("/api/v1/me/saved-searches", response_model=SavedSearchesResponse, tags=["user"])
def get_saved_searches(
    user_id: UUID = REQUIRED_USER_ID_DEPENDENCY,
    service: UserService = USER_SERVICE_DEPENDENCY,
) -> SavedSearchesResponse:
    return service.list_saved_searches(user_id)


@app.post(
    "/api/v1/me/saved-searches",
    response_model=SavedSearchItem,
    status_code=201,
    tags=["user"],
)
def create_saved_search(
    body: SavedSearchCreateRequest,
    user_id: UUID = REQUIRED_USER_ID_DEPENDENCY,
    service: UserService = USER_SERVICE_DEPENDENCY,
) -> SavedSearchItem:
    return service.create_saved_search(user_id, body)


@app.delete("/api/v1/me/saved-searches/{saved_search_id}", status_code=204, tags=["user"])
def delete_saved_search(
    saved_search_id: UUID,
    user_id: UUID = REQUIRED_USER_ID_DEPENDENCY,
    service: UserService = USER_SERVICE_DEPENDENCY,
) -> None:
    service.delete_saved_search(user_id, saved_search_id)
