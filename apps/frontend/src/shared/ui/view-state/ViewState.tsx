import type { ReactNode } from "react";

export type ViewStateKind = "loading" | "error" | "empty";

const LABELS: Record<ViewStateKind, string> = {
  loading: "Loading",
  error: "Attention",
  empty: "Empty",
};

export interface ViewStateProps {
  kind: ViewStateKind;
  title: string;
  message: string;
  detail?: string;
  actions?: ReactNode;
  compact?: boolean;
}

export function ViewState({
  kind,
  title,
  message,
  detail,
  actions,
  compact = false,
}: ViewStateProps) {
  return (
    <div className={`view-state view-state-${kind} ${compact ? "view-state-compact" : ""}`}>
      <span className="view-state-badge">{LABELS[kind]}</span>
      <strong>{title}</strong>
      <p>{message}</p>
      {detail ? <small>{detail}</small> : null}
      {actions ? <div className="view-state-actions">{actions}</div> : null}
    </div>
  );
}
