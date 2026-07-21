import { describe, expect, it } from "vitest";
import {
  extractTxtUpdateJobId,
  formatTxtUpdateFailureMessage,
  formatTxtUpdateStageLabel,
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

describe("formatTxtUpdateStageLabel", () => {
  it("shows explicit post-ingest maintenance stages", () => {
    expect(formatTxtUpdateStageLabel("feature_refresh: Refreshing confirmed ML features...")).toBe("補助特徴量を更新中");
    expect(formatTxtUpdateStageLabel("label_refresh: Refreshing mature 5/10/20-day labels...")).toBe("判定用ラベルを更新中");
    expect(formatTxtUpdateStageLabel("prediction_refresh: Refreshing confirmed ML predictions...")).toBe("予測データを更新中");
    expect(formatTxtUpdateStageLabel("cache_refresh: Refreshing rankings cache...")).toBe("ランキング/表示用データを更新中");
    expect(formatTxtUpdateStageLabel("analysis_backfill: Refreshing analysis backfill...")).toBe("分析履歴を補完中");
    expect(formatTxtUpdateStageLabel("tracking_refresh.basis")).toBe("追跡履歴を更新中");
  });

  it("shows a clear label when only post-processing is queued", () => {
    expect(formatTxtUpdateStageLabel("postprocess_resume: Queuing post-processing only...")).toBe("後処理のみ実行中");
    expect(formatTxtUpdateStageLabel("Pan/TXT import skipped; post-processing only")).toBe("後処理のみ実行中");
  });
});

describe("formatTxtUpdateFailureMessage", () => {
  it("returns readable Japanese failure messages", () => {
    expect(formatTxtUpdateFailureMessage("db_unavailable")).toContain("データベース");
    expect(formatTxtUpdateFailureMessage("code_txt_missing")).toContain("銘柄リスト");
    expect(formatTxtUpdateFailureMessage("vbs_not_found")).toContain("更新処理");
    expect(formatTxtUpdateFailureMessage("cancel")).toBe("日次更新は停止されました。");
  });
});
