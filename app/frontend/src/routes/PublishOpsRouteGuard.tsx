import type { ReactNode } from "react";
import { Navigate, useLocation } from "react-router-dom";
import { shouldShowOperatorConsole } from "../utils/operatorConsole";

type Props = {
  children: ReactNode;
};

export default function PublishOpsRouteGuard({ children }: Props) {
  const location = useLocation();

  if (!shouldShowOperatorConsole()) {
    return <Navigate to="/" replace state={{ from: location.pathname, reason: "operator-console-disabled" }} />;
  }

  return <>{children}</>;
}
