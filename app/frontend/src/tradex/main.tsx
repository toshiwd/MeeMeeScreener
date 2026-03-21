import React from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import ErrorBoundary from "../components/ErrorBoundary";
import TradexApp from "./TradexApp";
import "../styles.css";

const container = document.getElementById("root");

if (!container) {
  throw new Error("Root container not found");
}

createRoot(container).render(
  <React.StrictMode>
    <ErrorBoundary>
      <BrowserRouter basename="/tradex">
        <TradexApp />
      </BrowserRouter>
    </ErrorBoundary>
  </React.StrictMode>
);

