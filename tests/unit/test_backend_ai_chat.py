from __future__ import annotations

from fastapi.testclient import TestClient

from apps.backend.app.ai import AiChatRequest, AiChatResponse, AiChatService
from apps.backend.app.dependencies import get_ai_chat_service
from apps.backend.app.main import app


def test_ai_chat_service_returns_local_filter_plan_without_api_key(monkeypatch) -> None:
    monkeypatch.delenv("AI_CHAT_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    service = AiChatService()

    response = service.build_filter_plan(
        AiChatRequest(message="Подбери программирование в Москве, бюджет, очная форма, 85 баллов")
    )

    assert response.intent == "search"
    assert response.filters.city == "Москве"
    assert response.filters.direction == "программирование"
    assert response.filters.budget_type == "budget"
    assert response.filters.study_form == "full_time"
    assert response.filters.min_ege_score == 85
    assert response.model_used == "local-fallback"


def test_ai_chat_endpoint_uses_configured_service() -> None:
    class FakeAiChatService:
        def build_filter_plan(self, request: AiChatRequest) -> AiChatResponse:
            assert request.message == "Найди вузы в Казани"
            return AiChatResponse(
                intent="search",
                message_to_user="Готово.",
                filters={"query": "вузы", "city": "Казань", "country": "RU", "advanced": {}},
                missing_fields=[],
                confidence=0.9,
                model_used="fake-model",
            )

    app.dependency_overrides[get_ai_chat_service] = FakeAiChatService
    try:
        response = TestClient(app).post("/api/v1/ai/chat", json={"message": "Найди вузы в Казани"})
    finally:
        app.dependency_overrides.pop(get_ai_chat_service, None)

    assert response.status_code == 200
    payload = response.json()
    assert payload["message_to_user"] == "Готово."
    assert payload["filters"]["city"] == "Казань"
    assert payload["model_used"] == "fake-model"
