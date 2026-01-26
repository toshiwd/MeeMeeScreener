import type { ReactNode } from "react";

type IconButtonProps = {
  icon: ReactNode;
  label?: string;
  tooltip?: string;
  variant?: "icon" | "iconLabel";
  selected?: boolean;
  ariaLabel?: string;
  onClick?: () => void;
  disabled?: boolean;
  className?: string;
};

export default function IconButton({
  icon,
  label,
  tooltip,
  variant = "icon",
  selected,
  ariaLabel,
  onClick,
  disabled,
  className
}: IconButtonProps) {
  const title = tooltip ?? label ?? ariaLabel;
  const computedAriaLabel = ariaLabel ?? label ?? tooltip ?? "icon button";
  return (
    <button
      type="button"
      className={[
        "icon-button",
        variant === "iconLabel" ? "icon-button-label" : "",
        selected ? "is-selected" : "",
        className
      ]
        .filter(Boolean)
        .join(" ")}
      onClick={onClick}
      disabled={disabled}
      title={title}
      aria-label={computedAriaLabel}
    >
      <span className="icon-button-icon">{icon}</span>
      {variant === "iconLabel" && label ? (
        <span className="icon-button-text">{label}</span>
      ) : null}
    </button>
  );
}
