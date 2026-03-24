import { useEffect, useMemo, useState } from "react";
import { useAiExplain } from "./AiExplainProvider";
import {
  defaultAiExplainSettings,
  type AiExplainAnswerLength,
  type AiExplainSettingsDraft,
} from "./aiExplainApi";

const ANSWER_LENGTH_LABELS: Record<AiExplainAnswerLength, string> = {
  short: "短め",
  medium: "標準",
  long: "長め",
};

export default function AiExplainSettingsSection() {
  const { loading, error, state, settings, save, refresh } = useAiExplain();
  const [draft, setDraft] = useState<AiExplainSettingsDraft>({
    ...defaultAiExplainSettings(),
    authSecret: "",
    clearAuthSecret: false,
  });
  const [secretInput, setSecretInput] = useState("");
  const [saving, setSaving] = useState(false);
  const [savedMessage, setSavedMessage] = useState<string | null>(null);

  const providerTypeLabel = useMemo(() => draft.providerType || "openai_compatible", [draft.providerType]);

  useEffect(() => {
    setDraft((current) => ({
      ...current,
      ...settings,
      authSecret: "",
      clearAuthSecret: false,
    }));
    setSecretInput("");
  }, [settings]);

  const updateDraft = <K extends keyof AiExplainSettingsDraft>(key: K, value: AiExplainSettingsDraft[K]) => {
    setDraft((current) => ({ ...current, [key]: value }));
  };

  const handleSave = async () => {
    setSaving(true);
    setSavedMessage(null);
    try {
      await save({
        ...draft,
        authSecret: secretInput.trim() ? secretInput.trim() : null,
        clearAuthSecret: false,
      });
      setSecretInput("");
      setSavedMessage("設定を保存しました");
    } finally {
      setSaving(false);
    }
  };

  const handleClearSecret = async () => {
    setSaving(true);
    setSavedMessage(null);
    try {
      await save({
        ...draft,
        authSecret: null,
        clearAuthSecret: true,
      });
      setSecretInput("");
      setSavedMessage("認証情報を削除しました");
    } finally {
      setSaving(false);
    }
  };

  const readyLabel = state?.canUse ? "利用可" : loading ? "読み込み中" : "未設定";

  return (
    <div className="popover-section ai-explain-settings-section">
      <div className="popover-title">AI解説</div>
      <div className="popover-hint">
        一覧の設定画面で AI解説の表示ON/OFF と接続設定を切り替えます。Ranking / Detail 以外には出しません。
      </div>

      <div className="popover-input-row ai-explain-toggle-row">
        <button
          type="button"
          className={`popover-item ${draft.uiVisible ? "active" : ""}`}
          onClick={() => updateDraft("uiVisible", !draft.uiVisible)}
        >
          <span className="popover-item-label">AI解説を表示する</span>
        </button>
        <button
          type="button"
          className={`popover-item ${draft.enabled ? "active" : ""}`}
          onClick={() => updateDraft("enabled", !draft.enabled)}
        >
          <span className="popover-item-label">AI解説を有効化する</span>
        </button>
      </div>

      <div className="ai-explain-grid">
        <label className="ai-explain-field">
          <span className="popover-hint">Provider label</span>
          <input
            className="popover-input"
            type="text"
            placeholder="sakura"
            value={draft.providerLabel}
            onChange={(event) => updateDraft("providerLabel", event.target.value)}
          />
        </label>
        <label className="ai-explain-field">
          <span className="popover-hint">保存名</span>
          <input
            className="popover-input"
            type="text"
            placeholder="sakura"
            value={draft.credentialName}
            onChange={(event) => updateDraft("credentialName", event.target.value)}
          />
        </label>
        <label className="ai-explain-field ai-explain-field-wide">
          <span className="popover-hint">Endpoint</span>
          <input
            className="popover-input"
            type="text"
            placeholder="https://api.example.com/v1"
            value={draft.endpointUrl}
            onChange={(event) => updateDraft("endpointUrl", event.target.value)}
          />
        </label>
        <label className="ai-explain-field">
          <span className="popover-hint">Model</span>
          <input
            className="popover-input"
            type="text"
            placeholder="model-name"
            value={draft.model}
            onChange={(event) => updateDraft("model", event.target.value)}
          />
        </label>
        <label className="ai-explain-field">
          <span className="popover-hint">Provider type</span>
          <input className="popover-input" type="text" value={providerTypeLabel} readOnly />
        </label>
      </div>

      <div className="popover-input-row">
        <input
          className="popover-input"
          type="password"
          placeholder={state?.credentialConfigured ? "認証情報を再入力" : "認証情報"}
          value={secretInput}
          onChange={(event) => setSecretInput(event.target.value)}
        />
        <button type="button" className="popover-item" onClick={handleSave} disabled={saving}>
          {saving ? "保存中..." : "保存"}
        </button>
        <button
          type="button"
          className="popover-item"
          onClick={handleClearSecret}
          disabled={saving || !state?.credentialConfigured}
        >
          削除
        </button>
      </div>

      <div className="popover-input-row">
        <button
          type="button"
          className={`popover-item ${draft.sendImages ? "active" : ""}`}
          onClick={() => updateDraft("sendImages", !draft.sendImages)}
        >
          <span className="popover-item-label">画像を送る</span>
        </button>
        <button
          type="button"
          className={`popover-item ${draft.compareEnabled ? "active" : ""}`}
          onClick={() => updateDraft("compareEnabled", !draft.compareEnabled)}
        >
          <span className="popover-item-label">比較モード</span>
        </button>
        <button
          type="button"
          className={`popover-item ${draft.debugEnabled ? "active" : ""}`}
          onClick={() => updateDraft("debugEnabled", !draft.debugEnabled)}
        >
          <span className="popover-item-label">デバッグ表示</span>
        </button>
      </div>

      <div className="popover-input-row">
        <div className="segmented segmented-compact ai-explain-answer-length">
          {(["short", "medium", "long"] as AiExplainAnswerLength[]).map((value) => (
            <button
              key={value}
              type="button"
              className={draft.answerLength === value ? "active" : ""}
              onClick={() => updateDraft("answerLength", value)}
            >
              {ANSWER_LENGTH_LABELS[value]}
            </button>
          ))}
        </div>
        <input
          className="popover-input ai-explain-limit-input"
          type="number"
          min={0}
          max={1000}
          value={draft.dailyLimit}
          onChange={(event) => updateDraft("dailyLimit", Number(event.target.value) || 0)}
        />
        <span className="popover-status">1日上限</span>
      </div>

      <div className="popover-input-row">
        <button type="button" className="popover-item" onClick={() => void refresh()} disabled={loading}>
          {loading ? "読み込み中..." : "最新を取得"}
        </button>
        <span className="popover-status">{readyLabel}</span>
      </div>

      <div className="popover-hint ai-explain-state-hint">
        <span>{state?.providerReady ? "provider ready" : "provider pending"}</span>
        <span>{state?.credentialConfigured ? "secret saved" : "secret missing"}</span>
        <span>{state?.canShowUi ? "ui visible" : "ui hidden"}</span>
      </div>
      {savedMessage && <div className="popover-hint">{savedMessage}</div>}
      {error && <div className="popover-hint">設定取得エラー: {error}</div>}
    </div>
  );
}
