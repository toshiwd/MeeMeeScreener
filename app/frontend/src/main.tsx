import React from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import App from "./App";
import ErrorBoundary from "./components/ErrorBoundary";
import { installFrontendFatalDiagnosticsBridge } from "./utils/fatalDiagnostics";
import "./styles.css";

const container = document.getElementById("root");
if (!container) {
  throw new Error("Root container not found");
}

installFrontendFatalDiagnosticsBridge();

createRoot(container).render(
  <React.StrictMode>
    <ErrorBoundary>
      <BrowserRouter>
        <App />
      </BrowserRouter>
    </ErrorBoundary>
  </React.StrictMode>
);
