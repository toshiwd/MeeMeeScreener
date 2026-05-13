import { describe, expect, it } from "vitest";
import {
  extractTxtUpdateJobId,
  formatTxtUpdateStatusLabel,
  isTxtUpdateConflictError
} from "./txtUpdate";

describe("extractTxtUpdateJobId", () => {
  it("reads snake_case job id", () => {
    expect(extractTxtUpdateJobId({ job_id: "job-1" })).toBe("job-1");
  });

  it("reads camelCase job id", () => {
    expect(extractTxtUpdateJobId({ jobId: "job-2" })).toBe("job-2");
  });

  it("returns null when id is missing", () => {
    expect(extractTxtUpdateJobId({ ok: true })).toBeNull();
  });
});

describe("isTxtUpdateConflictError", () => {
  it("matches known conflict errors", () => {
    expect(isTxtUpdateConflictError("update_in_progress")).toBe(true);
    expect(isTxtUpdateConflictError("Job already running")).toBe(true);
  });

  it("returns false for other errors", () => {
    expect(isTxtUpdateConflictError("code_txt_missing")).toBe(false);
  });
});

describe("formatTxtUpdateStatusLabel", () => {
  it("returns Japanese label for known statuses", () => {
    expect(formatTxtUpdateStatusLabel("queued")).toBe("日次更新: 待機中");
    expect(formatTxtUpdateStatusLabel("running")).toBe("日次更新: 実行中");
    expect(formatTxtUpdateStatusLabel("cancel_requested")).toBe("日次更新: 停止中");
    expect(formatTxtUpdateStatusLabel("success")).toBe("日次更新: 完了");
    expect(formatTxtUpdateStatusLabel("failed")).toBe("日次更新: 失敗");
    expect(formatTxtUpdateStatusLabel("canceled")).toBe("日次更新: 停止済み");
  });

  it("returns a generic user-facing label for unknown statuses", () => {
    expect(formatTxtUpdateStatusLabel("queued_custom")).toBe("日次更新: 状態確認中");
  });
});
