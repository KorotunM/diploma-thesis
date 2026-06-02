from uuid import UUID

from fastapi import Depends, HTTPException, Query, status

from apps.backend.app.ai import AiChatProviderError, AiChatRequest, AiChatResponse, AiChatService
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
    get_ai_chat_service,
    get_auth_service,
    get_bearer_token,
    get_ege_subject_repository,
    get_location_suggest_service,
    get_optional_user_id,
    get_rankings_service,
    get_required_user_id,
    get_university_card_read_service,
    get_university_provenance_read_service,
    get_university_search_service,
    get_user_service,
)
from apps.backend.app.locations import LocationSuggestResponse, LocationSuggestService
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
AUTH_SERVICE_DEPENDENCY = Depends(get_auth_service)
AI_CHAT_SERVICE_DEPENDENCY = Depends(get_ai_chat_service)
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
PROGRAM_CODES_QUERY = Query(default=None)


@app.get("/api/v1/subjects", tags=["search"])
def get_ege_subjects(
    repo: EgeSubjectRepository = EGE_SUBJECTS_REPOSITORY_DEPENDENCY,
) -> dict:
    subjects = repo.list_all()
    return {"subjects": [{"id": s.code, "label": s.label} for s in subjects]}


@app.get("/api/v1/search", response_model=UniversitySearchResponse, tags=["search"])
def search_universities(
    query: str = "",
    city: str | None = None,
    region: str | None = None,
    country: str | None = None,
    source_type: str | None = None,
    ege_subjects: list[str] | None = EGE_SUBJECTS_QUERY,
    program_codes: list[str] | None = PROGRAM_CODES_QUERY,
    dormitory: bool = False,
    military_department: bool = False,
    sort_by: str = "rating",
    page: int = 1,
    page_size: int = 20,
    service: UniversitySearchService = SEARCH_SERVICE_DEPENDENCY,
) -> UniversitySearchResponse:
    return service.search(
        query,
        city=city,
        region=region,
        country=country,
        source_type=source_type,
        ege_subjects=ege_subjects,
        program_codes=program_codes,
        dormitory=dormitory,
        military_department=military_department,
        sort_by=sort_by,
        page=page,
        page_size=page_size,
    )


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


# ── University card ────────────────────────────────────────────────────────────

# --- AI chat ---------------------------------------------------------------

@app.post("/api/v1/ai/chat", response_model=AiChatResponse, tags=["ai"])
def ai_chat(
    body: AiChatRequest,
    service: AiChatService = AI_CHAT_SERVICE_DEPENDENCY,
) -> AiChatResponse:
    try:
        return service.build_filter_plan(body)
    except AiChatProviderError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=str(exc),
        ) from exc


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
