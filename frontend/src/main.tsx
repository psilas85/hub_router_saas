// src/main.tsx
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import "./index.css";
import "./styles/ui.css"; // 🔑 utilitários globais (btn, input, card...)
import App from "./App.tsx";

// 🔹 Toast notifications (feedbacks globais)
import { Toaster } from "react-hot-toast";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <>
      <App />
      {/* 🔹 Componente global de notificações */}
      <Toaster
        position="top-right"
        toastOptions={{
          style: {
            fontSize: "14px",
            borderRadius: "8px",
            padding: "12px 16px",
          },
          success: {
            style: { background: "#ecfdf5", color: "#065f46" }, // verde leve
          },
          error: {
            style: { background: "#fef2f2", color: "#991b1b" }, // vermelho leve
          },
        }}
      />
    </>
  </StrictMode>
);
