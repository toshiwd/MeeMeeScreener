import { useEffect } from "react";

type ToastAction = {
  label: string;
  onClick: () => void;
};

type ToastProps = {
  message: string | null;
  onClose: () => void;
  action?: ToastAction | null;
  duration?: number;
};

export default function Toast({ message, onClose, action, duration = 4000 }: ToastProps) {
  useEffect(() => {
    if (!message) return;
    // Always reset timer when message is set, even if it's the same string
    const timer = window.setTimeout(() => onClose(), duration);
    return () => window.clearTimeout(timer);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [message, duration]);

  const copyMessage = async () => {
    if (!message) return;
    try {
      await navigator.clipboard.writeText(message);
    } catch {
      // Ignore copy failures; the original toast is still visible.
    }
  };

  if (!message) return null;

  return (
    <div className="toast" role="status" aria-live="polite">
      <span className="toast-message">{message}</span>
      <button
        className="toast-action"
        onClick={(e) => {
          e.stopPropagation();
          void copyMessage();
        }}
      >
        コピー
      </button>
      {action && (
        <button
          className="toast-action"
          onClick={(e) => {
            e.stopPropagation();
            action.onClick();
          }}
        >
          {action.label}
        </button>
      )}
    </div>
  );
}
