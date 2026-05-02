import { SearchPage } from "./SearchPage";
import { UniversityCardPage } from "./UniversityCardPage";

export function SearchWorkspacePage() {
  return (
    <section className="workspace workspace--search">
      <div className="workspace__main">
        <SearchPage />
      </div>
      <aside className="workspace__side">
        <UniversityCardPage />
      </aside>
    </section>
  );
}
