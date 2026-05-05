import { useState } from "react";

import { HomePage } from "./HomePage";
import { EvidenceDrawer } from "../widgets/evidence-drawer/EvidenceDrawer";

type AdminTab = "monitoring" | "evidence";

const TABS: Array<{ id: AdminTab; label: string }> = [
  { id: "monitoring", label: "Мониторинг пайплайна" },
  { id: "evidence", label: "Доказательства" },
];

interface AdminDashboardProps {
  onLogout: () => void;
}

export function AdminDashboard({ onLogout }: AdminDashboardProps) {
  const [tab, setTab] = useState<AdminTab>("monitoring");

  return (
    <div className="admin-dashboard">
      <div className="admin-dashboard__topbar">
        <div className="admin-dashboard__topbar-inner">
          <div className="admin-dashboard__tabs">
            {TABS.map((t) => (
              <button
                key={t.id}
                className={`app__nav-link ${tab === t.id ? "app__nav-link--active" : ""}`}
                type="button"
                onClick={() => setTab(t.id)}
              >
                {t.label}
              </button>
            ))}
          </div>
          <button className="button button--ghost" type="button" onClick={onLogout}>
            Выйти
          </button>
        </div>
      </div>

      <div className="admin-dashboard__content">
        {tab === "monitoring" && <HomePage />}
        {tab === "evidence" && <EvidenceDrawer />}
      </div>
    </div>
  );
}
