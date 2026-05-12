export type ApiErrorInfo = {
  url: string;
  method: string;
  status: number | null;
  response: string | null;
  requestId: string | null;
  time: string;
};

export const formatApiErrorText = (info: ApiErrorInfo) => {
  const lines = [
    `接続先: ${info.url}`,
    `方法: ${info.method}`,
    `状態: ${info.status ?? "unknown"}`,
    `応答: ${info.response ?? "unknown"}`,
    `リクエストID: ${info.requestId ?? "unknown"}`,
    `時刻: ${info.time}`
  ];
  return lines.join("\n");
};
