import { useEffect, useMemo, useRef, useState } from "react";

import type {
  AiChatFiltersDto,
  AiChatMessageDto,
  AiChatUniversityDto,
} from "../shared/backend-api";
import { describeRequestError } from "../shared/http";
import { useFrontendRuntime } from "../shared/runtime";

type ChatRole = "user" | "assistant";

interface ChatMessage {
  id: string;
  role: ChatRole;
  content: string;
  createdAt: string;
  filters?: AiChatFiltersDto;
  intent?: "search" | "clarify" | "general";
  suggestions?: string[];
  universities?: AiChatUniversityDto[];
  modelUsed?: string | null;
  trialRemaining?: number | null;
}

const STORAGE_KEY = "abiturient_ai_chat_messages_v1";
const CLIENT_ID_KEY = "abiturient_ai_client_id_v1";
const MAX_MESSAGE_LENGTH = 500;

const QUICK_PROMPTS = [
  "Помоги подобрать вуз для программирования в Москве",
  "Найди бюджетные вузы с общежитием",
  "Подбери очное обучение при 85 баллах ЕГЭ",
];

const WELCOME_MESSAGE: ChatMessage = {
  id: "welcome",
  role: "assistant",
  content:
    "Привет! Напишите, какое направление, город, формат обучения и баллы ЕГЭ вас интересуют. Я подготовлю фильтры для поиска вузов.",
  createdAt: new Date().toISOString(),
};

export function AiChatWidget() {
  const { backendApi } = useFrontendRuntime();
  const [open, setOpen] = useState(false);
  const [input, setInput] = useState("");
  const [messages, setMessages] = useState<ChatMessage[]>(loadStoredMessages);
  const [sending, setSending] = useState(false);
  const listRef = useRef<HTMLDivElement | null>(null);

  const visibleMessages = messages.length > 0 ? messages : [WELCOME_MESSAGE];
  const charsLeft = MAX_MESSAGE_LENGTH - input.length;

  useEffect(() => {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(messages.slice(-60)));
  }, [messages]);

  useEffect(() => {
    if (!open) return;
    listRef.current?.scrollTo({ top: listRef.current.scrollHeight, behavior: "smooth" });
  }, [messages, open]);

  const requestHistory = useMemo<AiChatMessageDto[]>(
    () =>
      messages.slice(-10).map((message) => ({
        role: message.role,
        content: message.content,
      })),
    [messages],
  );

  const sendMessage = async (messageText = input) => {
    const normalized = messageText.trim().slice(0, MAX_MESSAGE_LENGTH);
    if (!normalized || sending) return;

    const userMessage: ChatMessage = {
      id: createMessageId(),
      role: "user",
      content: normalized,
      createdAt: new Date().toISOString(),
    };
    setMessages((current) => [...current, userMessage]);
    setInput("");
    setSending(true);

    try {
      const response = await backendApi.aiChat({
        message: normalized,
        history: requestHistory,
        client_id: getAiClientId(),
      });
      setMessages((current) => [
        ...current,
        {
          id: createMessageId(),
          role: "assistant",
          content: response.message_to_user,
          createdAt: new Date().toISOString(),
          filters: response.filters,
          intent: response.intent,
          suggestions: response.suggestions ?? [],
          universities: response.universities ?? [],
          modelUsed: response.model_used,
          trialRemaining: response.trial_remaining,
        },
      ]);
    } catch (error: unknown) {
      setMessages((current) => [
        ...current,
        {
          id: createMessageId(),
          role: "assistant",
          content: `Не удалось получить ответ ИИ: ${describeRequestError(error)}`,
          createdAt: new Date().toISOString(),
          intent: "general",
        },
      ]);
    } finally {
      setSending(false);
    }
  };

  return (
    <div className={`ai-chat ${open ? "ai-chat--open" : ""}`}>
      {!open && (
        <div className="ai-chat__launcher-wrap">
          <button className="ai-chat__launcher-label" type="button" onClick={() => setOpen(true)}>
            Спросить ИИ
          </button>
          <button
            className="ai-chat__launcher"
            type="button"
            aria-label="Открыть ИИ-помощник"
            onClick={() => setOpen(true)}
          >
            <span className="ai-chat__launcher-icon" aria-hidden>✦</span>
          </button>
        </div>
      )}

      {open && (
        <section className="ai-chat__panel" aria-label="ИИ-помощник">
          <header className="ai-chat__header">
            <div className="ai-chat__logo" aria-hidden>✦</div>
            <div className="ai-chat__title-wrap">
              <h2 className="ai-chat__title">ИИ-помощник</h2>
              <p className="ai-chat__subtitle">Помогу подобрать вуз, программу и ответить на вопросы</p>
            </div>
            <button
              className="ai-chat__close"
              type="button"
              aria-label="Закрыть ИИ-помощник"
              onClick={() => setOpen(false)}
            >
              ×
            </button>
          </header>

          <div className="ai-chat__messages" ref={listRef}>
            {visibleMessages.map((message) => (
              <article
                key={message.id}
                className={`ai-chat__message ai-chat__message--${message.role}`}
              >
                {message.role === "assistant" && (
                  <div className="ai-chat__message-logo" aria-hidden>✦</div>
                )}
                <div className="ai-chat__bubble">
                  <p>{message.content}</p>
                  {message.trialRemaining !== null && message.trialRemaining !== undefined && (
                    <small className="ai-chat__trial">
                      Пробных запросов осталось: {message.trialRemaining}
                    </small>
                  )}
                  {message.intent === "search" && message.filters && (
                    <>
                      {summarizeFilters(message.filters).length > 0 && (
                        <div className="ai-chat__filter-tags">
                          {summarizeFilters(message.filters).map((tag) => (
                            <span key={tag} className="ai-chat__filter-tag">{tag}</span>
                          ))}
                        </div>
                      )}
                      {message.universities && message.universities.length > 0 && (
                        <div className="ai-chat__unis">
                          {message.universities.map((uni) => (
                            <button
                              key={uni.university_id}
                              className="ai-chat__uni"
                              type="button"
                              title={uni.full_name ?? uni.name}
                              onClick={() => openUniversityCard(uni.university_id)}
                            >
                              <span className="ai-chat__uni-name">{uni.name}</span>
                              {uni.city && <span className="ai-chat__uni-city">{uni.city}</span>}
                              <span className="ai-chat__uni-go" aria-hidden>→</span>
                            </button>
                          ))}
                        </div>
                      )}
                      <button
                        className="ai-chat__apply"
                        type="button"
                        onClick={() => {
                          if (message.filters) applyFiltersToSearch(message.filters);
                        }}
                      >
                        Показать все в поиске
                        <span aria-hidden>›</span>
                      </button>
                    </>
                  )}
                  {message.suggestions && message.suggestions.length > 0 && (
                    <div className="ai-chat__suggestions">
                      {message.suggestions.map((suggestion) => (
                        <button
                          key={suggestion}
                          className="ai-chat__suggestion"
                          type="button"
                          disabled={sending}
                          onClick={() => void sendMessage(suggestion)}
                        >
                          {suggestion}
                        </button>
                      ))}
                    </div>
                  )}
                  <time dateTime={message.createdAt}>{formatMessageTime(message.createdAt)}</time>
                </div>
              </article>
            ))}
            {sending && (
              <article className="ai-chat__message ai-chat__message--assistant">
                <div className="ai-chat__message-logo" aria-hidden>✦</div>
                <div className="ai-chat__bubble ai-chat__bubble--typing">
                  Подбираю фильтры...
                </div>
              </article>
            )}
          </div>

          <div className="ai-chat__quick-actions">
            {QUICK_PROMPTS.map((prompt) => (
              <button key={prompt} type="button" onClick={() => sendMessage(prompt)}>
                {prompt}
              </button>
            ))}
          </div>

          <form
            className="ai-chat__composer"
            onSubmit={(event) => {
              event.preventDefault();
              void sendMessage();
            }}
          >
            <input
              value={input}
              maxLength={MAX_MESSAGE_LENGTH}
              placeholder="Напишите сообщение..."
              onChange={(event) => setInput(event.target.value)}
            />
            <span className="ai-chat__counter">{Math.max(0, charsLeft)}</span>
            <button type="submit" disabled={sending || input.trim().length === 0}>
              ↑
            </button>
          </form>
        </section>
      )}
    </div>
  );
}

function openUniversityCard(universityId: string): void {
  const url = new URL(window.location.href);
  url.searchParams.set("university_id", universityId);
  window.history.replaceState({}, "", url);
  window.location.hash = "university";
  window.dispatchEvent(new PopStateEvent("popstate"));
}

function applyFiltersToSearch(filters: AiChatFiltersDto): void {
  const url = new URL(window.location.href);
  const params = url.searchParams;

  // The text query is reserved for a university name/abbreviation; a study
  // direction is applied via program_codes instead.
  writeParam(params, "query", filters.query);
  params.delete("program_codes");
  for (const code of filters.program_codes ?? []) params.append("program_codes", code);

  writeParam(params, "city", filters.city);
  writeParam(params, "region", filters.region);
  writeParam(params, "country", filters.country);
  writeParam(params, "source_type", filters.source_type);

  // Visible checkbox filters.
  writeBoolean(params, "dormitory", filters.dormitory ?? filters.advanced?.dormitory ?? null);
  writeBoolean(params, "military_department", filters.military_department ?? null);

  // Hidden/advanced EGE panel — subjects and per-subject scores.
  const subjects = new Set(filters.ege_subjects ?? []);
  for (const subject of Object.keys(filters.ege_scores ?? {})) subjects.add(subject);
  params.delete("ege_subjects");
  for (const subject of subjects) params.append("ege_subjects", subject);
  params.delete("ege_scores");
  for (const [subject, score] of Object.entries(filters.ege_scores ?? {})) {
    if (Number.isFinite(score)) params.append("ege_scores", `${subject}:${score}`);
  }

  // Sort order.
  if (filters.sort_by && filters.sort_by !== "rating") {
    params.set("sort_by", filters.sort_by);
  } else {
    params.delete("sort_by");
  }

  params.delete("page");
  window.history.replaceState({}, "", url);
  if (window.location.hash !== "#search") {
    window.location.hash = "search";
  }
  window.dispatchEvent(new PopStateEvent("popstate"));
}

function writeBoolean(params: URLSearchParams, key: string, value: boolean | null): void {
  if (value === true) {
    params.set(key, "1");
  } else {
    params.delete(key);
  }
}

const SORT_LABELS: Record<string, string> = {
  rating: "сначала сильные",
  budget_places: "больше бюджетных мест",
  avg_passing_score: "ниже проходной балл",
};

const DIRECTION_LABELS: Record<string, string> = {
  it: "IT и программирование",
  engineering: "Инженерия",
  economy: "Экономика",
  medicine: "Медицина",
  management: "Управление",
  humanities: "Гуманитарные науки",
};

function summarizeFilters(filters: AiChatFiltersDto): string[] {
  const tags: string[] = [];
  if (filters.query?.trim()) tags.push(filters.query.trim());
  const direction = filters.direction?.trim();
  if (direction) tags.push(DIRECTION_LABELS[direction] ?? direction);
  if (filters.city?.trim()) tags.push(filters.city.trim());
  if (filters.region?.trim()) tags.push(filters.region.trim());
  const subjects = new Set(filters.ege_subjects ?? []);
  for (const subject of Object.keys(filters.ege_scores ?? {})) subjects.add(subject);
  for (const subject of subjects) {
    const score = filters.ege_scores?.[subject];
    tags.push(score ? `${subject} ${score}` : subject);
  }
  if (filters.dormitory === true || filters.advanced?.dormitory === true) tags.push("общежитие");
  if (filters.military_department === true) tags.push("военная кафедра");
  if (filters.sort_by && SORT_LABELS[filters.sort_by]) tags.push(SORT_LABELS[filters.sort_by]);
  return tags;
}

function writeParam(params: URLSearchParams, key: string, value: string | null | undefined): void {
  const normalized = value?.trim() ?? "";
  if (normalized) {
    params.set(key, normalized);
    return;
  }
  params.delete(key);
}

function loadStoredMessages(): ChatMessage[] {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw) as ChatMessage[];
    if (!Array.isArray(parsed)) return [];
    return parsed.filter(isStoredMessage).slice(-60);
  } catch {
    return [];
  }
}

function isStoredMessage(value: unknown): value is ChatMessage {
  if (typeof value !== "object" || value === null) return false;
  const candidate = value as Partial<ChatMessage>;
  return (
    (candidate.role === "user" || candidate.role === "assistant") &&
    typeof candidate.content === "string" &&
    typeof candidate.createdAt === "string" &&
    typeof candidate.id === "string"
  );
}

function createMessageId(): string {
  return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

function getAiClientId(): string {
  const existing = localStorage.getItem(CLIENT_ID_KEY);
  if (existing) return existing;
  const generated =
    window.crypto?.randomUUID?.() ?? `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  localStorage.setItem(CLIENT_ID_KEY, generated);
  return generated;
}

function formatMessageTime(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "";
  return new Intl.DateTimeFormat("ru-RU", {
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}
