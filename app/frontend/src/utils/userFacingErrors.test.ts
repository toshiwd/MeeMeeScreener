import { describe, expect, it } from "vitest";
import { formatUserFacingOperationIssue } from "./userFacingErrors";

describe("formatUserFacingOperationIssue", () => {
  it("hides internal timeout codes", () => {
    expect(formatUserFacingOperationIssue("refresh_timeout")).toBe(
      "更新確認に時間がかかっています"
    );
  });

  it("hides database implementation details", () => {
    expect(formatUserFacingOperationIssue("database is locked")).toBe(
      "データベースが混み合っています"
    );
  });

  it("hides generic programming-style failures", () => {
    expect(formatUserFacingOperationIssue("request failed: HTTP 503")).toBe(
      "更新を確認できませんでした"
    );
  });

  it("hides internal daily update file names", () => {
    expect(formatUserFacingOperationIssue("code_txt_missing")).toBe("必要な銘柄リストが見つかりません");
    expect(formatUserFacingOperationIssue("vbs_not_found")).toBe("日次更新に必要な更新処理が見つかりません");
  });

  it("keeps already user-facing Japanese messages", () => {
    expect(formatUserFacingOperationIssue("日次更新を確認しています")).toBe(
      "日次更新を確認しています"
    );
  });

  it("returns null for empty input", () => {
    expect(formatUserFacingOperationIssue(null)).toBeNull();
    expect(formatUserFacingOperationIssue("")).toBeNull();
  });
});
