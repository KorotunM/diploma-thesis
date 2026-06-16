import { useEffect, useMemo, useRef, useState } from "react";

import type { AiChatFiltersDto, AiChatMessageDto } from "../shared/backend-api";
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
                    <button
                      className="ai-chat__apply"
                      type="button"
                      onClick={() => {
                        if (message.filters) applyFiltersToSearch(message.filters);
                      }}
                    >
                      Показать в поиске
                      <span aria-hidden>›</span>
                    </button>
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

function applyFiltersToSearch(filters: AiChatFiltersDto): void {
  const url = new URL(window.location.href);

  const queryText =
    filters.query?.trim() ||
    filters.direction?.trim() ||
    filters.advanced?.program_query?.trim() ||
    null;

  writeParam(url.searchParams, "query", queryText);
  writeParam(url.searchParams, "city", filters.city);
  writeParam(url.searchParams, "country", filters.country);
  writeParam(url.searchParams, "source_type", filters.source_type);
  if (filters.advanced?.dormitory === true) {
    url.searchParams.set("dormitory", "true");
  } else {
    url.searchParams.delete("dormitory");
  }
  url.searchParams.delete("page");
  window.history.replaceState({}, "", url);
  if (window.location.hash !== "#search") {
    window.location.hash = "search";
  }
  window.dispatchEvent(new PopStateEvent("popstate"));
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
