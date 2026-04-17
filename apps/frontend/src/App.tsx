import { EvidenceDrawer } from "./widgets/evidence-drawer/EvidenceDrawer";
import { HomePage } from "./pages/HomePage";
import { SearchPage } from "./pages/SearchPage";
import { UniversityCardPage } from "./pages/UniversityCardPage";

export default function App() {
  return (
    <div className="app-shell">
      <header className="hero">
        <p className="eyebrow">Evidence-first university aggregator</p>
        <h1>Foundation UI for search, cards and provenance.</h1>
        <p className="hero-copy">
          This frontend is intentionally thin. It mirrors the target product areas
          without locking us into premature page logic.
        </p>
      </header>

      <main className="grid">
        <HomePage />
        <SearchPage />
        <UniversityCardPage />
        <EvidenceDrawer />
      </main>
    </div>
  );
}
