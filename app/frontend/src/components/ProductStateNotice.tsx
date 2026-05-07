import type { ReactNode } from "react";

export type ProductStateNoticeKind =
  | "loading"
  | "error"
  | "empty"
  | "missing"
  | "stale"
  | "db_busy";

type Props = {
  kind: ProductStateNoticeKind;
  children: ReactNode;
  className?: string;
  prefix?: string;
};

export default function ProductStateNotice({
  kind,
  children,
  className,
  prefix,
}: Props) {
  const classes = ["product-state-notice", `is-${kind}`];
  if (className) classes.push(className);
  return (
    <div className={classes.join(" ")} data-product-state={kind}>
      {prefix ? <span className="product-state-prefix">{prefix}: </span> : null}
      <span className="product-state-message">{children}</span>
    </div>
  );
}
