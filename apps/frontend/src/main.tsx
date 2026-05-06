import React from "react";
import ReactDOM from "react-dom/client";

import App from "./App";
import { AuthProvider } from "./shared/auth";
import { FrontendRuntimeProvider, createFrontendRuntime } from "./shared/runtime";
import { SelectedUniversityProvider } from "./shared/selected-university";
import "./styles/main.scss";

const runtime = createFrontendRuntime();

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <FrontendRuntimeProvider value={runtime}>
      <AuthProvider backendBaseUrl={runtime.config.backendBaseUrl}>
        <SelectedUniversityProvider>
          <App />
        </SelectedUniversityProvider>
      </AuthProvider>
    </FrontendRuntimeProvider>
  </React.StrictMode>,
);
