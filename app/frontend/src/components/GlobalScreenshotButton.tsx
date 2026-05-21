import { useEffect, useState } from "react";
import { useLocation, useParams } from "react-router-dom";
import { IconAlertCircle, IconCamera, IconCheck, IconLoader2 } from "@tabler/icons-react";
import { captureAndCopyScreenshot, getScreenType, saveBlobToFile } from "../utils/windowScreenshot";

type ButtonState = "idle" | "saving" | "saved" | "failed";

const resetDelayMs = 1800;

export default function GlobalScreenshotButton() {
  const location = useLocation();
  const { code } = useParams<{ code?: string }>();
  const [state, setState] = useState<ButtonState>("idle");

  useEffect(() => {
    if (state !== "saved" && state !== "failed") return undefined;
    const timer = window.setTimeout(() => setState("idle"), resetDelayMs);
    return () => window.clearTimeout(timer);
  }, [state]);

  const busy = state === "saving";
  const label =
    state === "saving"
      ? "スクショコピー中"
      : state === "saved"
        ? "スクショコピー済み"
        : state === "failed"
          ? "スクショ失敗"
          : "画面スクショをコピー";
  const Icon =
    state === "saving"
      ? IconLoader2
      : state === "saved"
        ? IconCheck
        : state === "failed"
          ? IconAlertCircle
          : IconCamera;

  const handleClick = async () => {
    if (busy) return;
    setState("saving");
    try {
      const capture = await captureAndCopyScreenshot({
        screenType: getScreenType(location.pathname),
        code,
      });
      if (!capture.success || !capture.blob || !capture.filename) {
        setState("failed");
        return;
      }
      if (capture.copied) {
        setState("saved");
        return;
      }
      const save = await saveBlobToFile(capture.blob, capture.filename);
      setState(save.success ? "saved" : "failed");
    } catch {
      setState("failed");
    }
  };

  return (
    <button
      type="button"
      className={`global-screenshot-button state-${state}`}
      data-html2canvas-ignore="true"
      aria-label={label}
      title={label}
      disabled={busy}
      onClick={handleClick}
    >
      <Icon size={20} aria-hidden="true" />
    </button>
  );
}
