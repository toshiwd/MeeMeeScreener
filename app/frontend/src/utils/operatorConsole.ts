export function shouldShowOperatorConsole(flag = import.meta.env.VITE_SHOW_OPERATOR_CONSOLE) {
  return flag !== "0";
}
