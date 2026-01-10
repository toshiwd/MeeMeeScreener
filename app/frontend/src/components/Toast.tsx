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
    const timer = window.setTimeout(() => onClose(), duration);
    return () => window.clearTimeout(timer);
  }, [message, onClose, duration]);

  if (!message) return null;

  return (
    <div className="toast" role="status" aria-live="polite">
      <span className="toast-message">{message}</span>
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
