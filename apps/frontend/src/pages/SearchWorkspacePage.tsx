import { SearchPage } from "./SearchPage";

export function SearchWorkspacePage({ onShowLogin }: { onShowLogin?: () => void }) {
  return <SearchPage onShowLogin={onShowLogin} />;
}
