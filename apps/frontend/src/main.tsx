import React from "react";
import ReactDOM from "react-dom/client";

import App from "./App";
import { FrontendRuntimeProvider, createFrontendRuntime } from "./shared/runtime";
import "./styles.css";

const runtime = createFrontendRuntime();

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <FrontendRuntimeProvider value={runtime}>
      <App />
    </FrontendRuntimeProvider>
  </React.StrictMode>,
);
