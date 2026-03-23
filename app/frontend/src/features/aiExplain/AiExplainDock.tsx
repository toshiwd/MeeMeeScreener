import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { CSSProperties } from "react";
import { IconChevronDown, IconChevronUp, IconSparkles, IconSend, IconX } from "@tabler/icons-react";
import ScreenPanel from "../../components/ScreenPanel";
import { useAiExplain } from "./AiExplainProvider";
import {
  streamAiExplain,
  type AiExplainMode,
  type AiExplainScreenType,
} from "./aiExplainApi";

type AiExplainQuickAction = {
  label: string;
  question: string;
  mode: AiExplainMode;
};

type AiExplainDockProps = {
  screenType: AiExplainScreenType;
  targetLabel: string;
  snapshot: Record<string, unknown>;
  images?: string[];
  compareLabel?: string | null;
  bottomOffsetPx?: number;
  className?: string;
};

const buildDefaultQuestion = (
  screenType: AiExplainScreenType,
  targetLabel: string,
  compareEnabled: boolean,
  compareLabel?: string | null
) => {
  if (screenType === "ranking") {
    return "なぜこの銘柄が上位なのか";
  }
  if (compareEnabled && compareLabel) {
    return `${targetLabel} と ${compareLabel} の違いは何か`;
  }
  return `${targetLabel} の今後の展望と注意点をまとめて`;
};

const buildDefaultMode = (
  screenType: AiExplainScreenType,
  compareEnabled: boolean,
  compareLabel?: string | null
): AiExplainMode => {
  if (screenType === "ranking") return "explain";
  if (compareEnabled && compareLabel) return "compare";
  return "explain";
};

export default function AiExplainDock({
  screenType,
  targetLabel,
  snapshot,
  images = [],
  compareLabel = null,
  bottomOffsetPx = 18,
  className,
}: AiExplainDockProps) {
  const { canShowUi, canUse, settings } = useAiExplain();
  const enabled = canShowUi && canUse;
  const [open, setOpen] = useState(false);
  const [question, setQuestion] = useState(() =>
    buildDefaultQuestion(screenType, targetLabel, Boolean(settings.compareEnabled), compareLabel)
  );
  const [answer, setAnswer] = useState("");
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [cached, setCached] = useState(false);
  const [provider, setProvider] = useState<string>("");
  const [model, setModel] = useState<string>("");
  const [latencyMs, setLatencyMs] = useState<number | null>(null);
  const [loading, setLoading] = useState(false);
  const [lastQuestion, setLastQuestion] = useState<string | null>(null);
  const [lastMode, setLastMode] = useState<AiExplainMode>(
    buildDefaultMode(screenType, Boolean(settings.compareEnabled), compareLabel)
  );
  const abortRef = useRef<AbortController | null>(null);
  const panelRef = useRef<HTMLDivElement | null>(null);

  const requestImages = useMemo(
    () => (settings.sendImages ? images.slice(0, 3) : []),
    [images, settings.sendImages]
  );

  const quickActions = useMemo<AiExplainQuickAction[]>(() => {
    if (screenType === "ranking") {
      return [
        { label: "なぜ上位？", question: "なぜこの銘柄が上位なのか", mode: "explain" },
        { label: "順位差の主因", question: "上位銘柄の順位差の主因は何か", mode: "summarize" },
        { label: "注意点", question: "今のランキング結果に対する注意点は何か", mode: "summarize" },
      ];
    }
    if (compareLabel && settings.compareEnabled) {
      return [
        {
          label: "この2銘柄の違い",
          question: `${targetLabel} と ${compareLabel} の違いは何か`,
          mode: "compare",
        },
        {
          label: "順位差の主因",
          question: `${targetLabel} と ${compareLabel} の順位差の主因は何か`,
          mode: "compare",
        },
        {
          label: "チャート形状の違い",
          question: `${targetLabel} と ${compareLabel} のチャート形状の違いは何か`,
          mode: "compare",
        },
        {
          label: "注意点",
          question: `${targetLabel} の注意点は何か`,
          mode: "explain",
        },
      ];
    }
    return [
      { label: "今後の展望", question: `${targetLabel} の今後の展望をまとめて`, mode: "explain" },
      { label: "注意点", question: `${targetLabel} の注意点をまとめて`, mode: "explain" },
      { label: "要約", question: `${targetLabel} を今後の見通しも含めて要約して`, mode: "summarize" },
    ];
  }, [compareLabel, screenType, settings.compareEnabled, targetLabel]);

  const cancelInFlight = useCallback(() => {
    abortRef.current?.abort();
    abortRef.current = null;
  }, []);

  const submit = useCallback(
    async (modeOverride?: AiExplainMode, questionOverride?: string) => {
      if (!enabled || loading) return;
      const nextQuestion =
        (questionOverride ?? question).trim() ||
        buildDefaultQuestion(screenType, targetLabel, Boolean(settings.compareEnabled), compareLabel);
      const mode = modeOverride ?? buildDefaultMode(screenType, Boolean(settings.compareEnabled), compareLabel);
      cancelInFlight();
      const controller = new AbortController();
      abortRef.current = controller;
      setLoading(true);
      setErrorMessage(null);
      setAnswer("");
      setCached(false);
      setProvider("");
      setModel("");
      setLatencyMs(null);
      setLastQuestion(nextQuestion);
      setLastMode(mode);
      try {
        const result = await streamAiExplain(
          {
            mode,
            screenType,
            userQuestion: nextQuestion,
            snapshot,
            images: requestImages,
          },
          controller.signal,
          {
            onDelta: (delta) => {
              setAnswer((current) => current + delta);
            },
          }
        );
        if (controller.signal.aborted) return;
        setAnswer(result.answer);
        setCached(result.cached);
        setProvider(result.provider);
        setModel(result.model);
        setLatencyMs(result.latencyMs);
        setErrorMessage(result.error?.message ?? null);
      } catch (err) {
        if (controller.signal.aborted) return;
        setErrorMessage(err instanceof Error ? err.message : "AI解説を取得できませんでした");
      } finally {
        if (abortRef.current === controller) {
          abortRef.current = null;
        }
        setLoading(false);
      }
    },
    [cancelInFlight, compareLabel, enabled, loading, question, requestImages, screenType, settings.compareEnabled, snapshot, targetLabel]
  );

  useEffect(() => () => cancelInFlight(), [cancelInFlight]);

  useEffect(() => {
    setQuestion(buildDefaultQuestion(screenType, targetLabel, Boolean(settings.compareEnabled), compareLabel));
    setLastMode(buildDefaultMode(screenType, Boolean(settings.compareEnabled), compareLabel));
  }, [compareLabel, screenType, settings.compareEnabled, targetLabel]);

  useEffect(() => {
    if (!open) return;
    const body = panelRef.current?.querySelector(".screen-panel-body") as HTMLDivElement | null;
    if (!body) return;
    body.scrollTop = body.scrollHeight;
  }, [answer, loading, open]);

  if (!enabled) return null;
  const displayErrorMessage =
    errorMessage && settings.debugEnabled ? `AI解説を取得できない: ${errorMessage}` : "AI解説を取得できない";

  const handleToggleOpen = () => {
    if (open) {
      cancelInFlight();
      setOpen(false);
      return;
    }
    setOpen(true);
  };

  return (
    <div
      className={["ai-explain-dock", open ? "is-open" : "is-closed", className].filter(Boolean).join(" ")}
      style={{ bottom: `${bottomOffsetPx}px` } as CSSProperties}
    >
      <button
        type="button"
        className="ai-explain-fab"
        onClick={handleToggleOpen}
        aria-expanded={open}
        aria-label={open ? "AI解説を閉じる" : "AI解説を開く"}
      >
        <IconSparkles size={16} />
        <span>{open ? "閉じる" : "AI解説"}</span>
      </button>

      {open && (
        <ScreenPanel
          ref={panelRef}
          title="AI解説"
          summary={`対象: ${targetLabel}`}
          details={`${settings.providerLabel || "sakura"} / ${settings.model || "--"}${cached ? " / cached" : ""}`}
          actions={
            <button
              type="button"
              className="ai-explain-close"
              onClick={() => {
                cancelInFlight();
                setOpen(false);
              }}
            >
              <IconX size={14} />
              <span>閉じる</span>
            </button>
          }
          className="ai-explain-panel"
        >
          <div className="ai-explain-quick-actions">
            {quickActions.map((action) => (
              <button
                key={action.label}
                type="button"
                className="ai-explain-action-button"
                onClick={() => void submit(action.mode, action.question)}
                disabled={loading}
              >
                {action.label}
              </button>
            ))}
          </div>

          <div className="ai-explain-input-row">
            <textarea
              className="ai-explain-input"
              rows={2}
              value={question}
              onChange={(event) => setQuestion(event.target.value)}
              placeholder="質問を入力"
            />
            <button
              type="button"
              className="ai-explain-send"
              onClick={() => void submit()}
              disabled={loading}
            >
              <IconSend size={14} />
              <span>{loading ? "送信中..." : "送信"}</span>
            </button>
          </div>

          <div className="ai-explain-answer-meta">
            <span>{lastQuestion ? `質問: ${lastQuestion}` : "質問待ち"}</span>
            <span>{latencyMs != null ? `${latencyMs}ms` : "--"}</span>
            <span>{provider || "--"}</span>
            <span>{model || "--"}</span>
            <span>{lastMode}</span>
          </div>

          {loading && <div className="ai-explain-status">AI解説を生成中...</div>}
          {errorMessage && !loading && (
            <div className="ai-explain-status ai-explain-status-error">{displayErrorMessage}</div>
          )}
          {(answer || loading) && <div className="ai-explain-answer">{answer}</div>}

          {answer && !loading && (
            <div className="ai-explain-more-row">
              <button
                type="button"
                className="ai-explain-more"
                onClick={() =>
                  void submit(
                    lastMode,
                    `${(lastQuestion ??
                      (question.trim() ||
                        buildDefaultQuestion(screenType, targetLabel, Boolean(settings.compareEnabled), compareLabel)))}。もう少し詳しく、要点だけ補ってください。`
                  )
                }
                disabled={loading}
              >
                <IconChevronDown size={14} />
                <span>もう少し詳しく</span>
              </button>
              <button
                type="button"
                className="ai-explain-more"
                onClick={() => {
                  const nextQuestion = buildDefaultQuestion(
                    screenType,
                    targetLabel,
                    Boolean(settings.compareEnabled),
                    compareLabel
                  );
                  setQuestion(nextQuestion);
                  void submit(lastMode, nextQuestion);
                }}
                disabled={loading}
              >
                <IconChevronUp size={14} />
                <span>初期質問に戻す</span>
              </button>
            </div>
          )}
        </ScreenPanel>
      )}
    </div>
  );
}
