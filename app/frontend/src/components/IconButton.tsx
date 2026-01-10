import type { ReactNode } from "react";

type IconButtonProps = {
  icon: ReactNode;
  label: string;
  onClick?: () => void;
  disabled?: boolean;
  className?: string;
  title?: string;
};

export default function IconButton({
  icon,
  label,
  onClick,
  disabled,
  className,
  title
}: IconButtonProps) {
  const tooltip = title ?? label;
  return (
    <button
      type="button"
      className={["icon-button", className].filter(Boolean).join(" ")}
      onClick={onClick}
      disabled={disabled}
      title={tooltip}
      aria-label={label}
    >
      <span className="icon-button-icon">{icon}</span>
    </button>
  );
}
