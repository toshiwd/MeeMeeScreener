const OPERATOR_CONSOLE_GATE_MODE = import.meta.env.VITE_OPERATOR_CONSOLE_GATE_MODE;

export const OPERATOR_CONSOLE_GATE_HEADER_NAME = "X-MeeMee-Operator-Mode";
export const OPERATOR_CONSOLE_GATE_HEADER_VALUE = "operator";

export function shouldShowOperatorConsole(flag = import.meta.env.VITE_SHOW_OPERATOR_CONSOLE) {
  return flag !== "0";
}

export function shouldAttachOperatorConsoleHeader(flag = OPERATOR_CONSOLE_GATE_MODE) {
  return String(flag ?? "").trim().toLowerCase() === "header";
}

export function attachOperatorConsoleHeader(headers: HeadersInit | undefined = undefined): Headers {
  const next = new Headers(headers ?? {});
  if (shouldAttachOperatorConsoleHeader() && !next.has(OPERATOR_CONSOLE_GATE_HEADER_NAME)) {
    next.set(OPERATOR_CONSOLE_GATE_HEADER_NAME, OPERATOR_CONSOLE_GATE_HEADER_VALUE);
  }
  return next;
}
