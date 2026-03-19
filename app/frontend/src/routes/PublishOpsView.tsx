import { useCallback, useEffect, useMemo, useState } from "react";

type AnyRecord = Record<string, unknown>;

type RuntimeSelectionSnapshot = {
  resolved_source?: string | null;
  selected_logic_id?: string | null;
  selected_logic_version?: string | null;
  logic_key?: string | null;
  artifact_uri?: string | null;
  source_of_truth?: string | null;
  degraded?: boolean;
  bootstrap_rule?: string | null;
  selected_logic_override?: unknown;
  last_known_good?: unknown;
  last_known_good_present?: boolean;
  override_present?: boolean;
  last_sync_time?: string | null;
  registry_sync_state?: string | null;
  maintenance_state?: AnyRecord | null;
  candidate_backfill_last_run?: AnyRecord | null;
  snapshot_sweep_last_run?: AnyRecord | null;
  non_promotable_legacy_count?: number | null;
  maintenance_degraded?: boolean;
};

type PublishStateSnapshot = {
  source_of_truth?: string | null;
  registry_sync_state?: string | null;
  degraded?: boolean;
  last_sync_time?: string | null;
  bootstrap_rule?: string | null;
  default_logic_pointer?: string | null;
  champion?: AnyRecord | null;
  challengers?: AnyRecord[];
  champion_logic_key?: string | null;
  challenger_logic_keys?: string[];
  previous_stable_champion_logic_key?: string | null;
  external_registry_version?: string | null;
  local_mirror_version?: string | null;
  mirror_schema_version?: string | null;
  mirror_normalized?: boolean;
  candidate_backfill_last_run?: AnyRecord | null;
  snapshot_sweep_last_run?: AnyRecord | null;
  non_promotable_legacy_count?: number | null;
  maintenance_degraded?: boolean;
  maintenance_state?: AnyRecord | null;
};

type CandidateBundle = {
  candidate_id: string;
  logic_key: string;
  logic_id?: string | null;
  logic_version?: string | null;
  logic_family?: string | null;
  status?: string | null;
  validation_state?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
  published_logic_manifest?: AnyRecord | null;
  validation_summary?: AnyRecord | null;
  published_logic_artifact?: AnyRecord | null;
  published_ranking_snapshot?: AnyRecord | null;
};

type CandidateRow = {
  candidateId: string;
  logicKey: string;
  status: string;
  validationState: string;
  createdAt: string;
  updatedAt: string;
  readinessPass: boolean;
  sampleCount: number | null;
  expectancyDelta: number | null;
  hasSnapshot: boolean;
};

const KEEP_APPROVED_DAYS = 90;
const KEEP_REJECTED_DAYS = 14;
const KEEP_RETIRED_DAYS = 14;

async function fetchJson<T>(url: string, init?: RequestInit): Promise<T> {
  const headers = new Headers(init?.headers ?? {});
  if (init?.body && !headers.has("Content-Type")) headers.set("Content-Type", "application/json");
  const response = await fetch(url, { ...init, headers });
  const text = await response.text();
  let payload: unknown = null;
  if (text) {
    try {
      payload = JSON.parse(text) as unknown;
    } catch {
      payload = text;
    }
  }
  if (!response.ok) {
    const detail = payload && typeof payload === "object" ? (payload as AnyRecord).detail : null;
    const reason = payload && typeof payload === "object" ? (payload as AnyRecord).reason : null;
    const message = typeof detail === "string" ? detail : typeof reason === "string" ? reason : `${response.status} ${response.statusText}`;
    throw new Error(message);
  }
  return payload as T;
}

function text(value: unknown, fallback = "N/A"): string {
  if (value === null || value === undefined) return fallback;
  const str = String(value).trim();
  return str.length ? str : fallback;
}

function boolText(value: unknown): string {
  if (value === true) return "true";
  if (value === false) return "false";
  return "N/A";
}

function numText(value: unknown): string {
  if (typeof value === "number" && Number.isFinite(value)) return value.toLocaleString();
  if (typeof value === "string" && value.trim()) return value;
  return "N/A";
}

function parseNum(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function obj(value: unknown): AnyRecord | null {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as AnyRecord) : null;
}

function badgeClass(value: string | null | undefined): string {
  const v = String(value ?? "").toLowerCase();
  if (!v) return "is-neutral";
  if (["true", "ok", "ready", "in_sync", "external_analysis", "healthy"].includes(v)) return "is-ok";
  if (["warn", "warning", "mirror_stale", "mirror_legacy", "local_mirror", "mirror", "candidate", "pending"].includes(v)) return "is-warn";
  if (["error", "danger", "invalid", "external_invalid", "external_unreachable", "rejected", "retired", "demoted", "degraded"].includes(v)) {
    return "is-danger";
  }
  return "is-neutral";
}

function prettyJson(value: unknown): string {
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

function shortValue(value: unknown): string {
  if (value === undefined) return "N/A";
  if (value === null) return "null";
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") return String(value);
  if (Array.isArray(value)) return `array(${value.length})`;
  if (value && typeof value === "object") return `object(${Object.keys(value as AnyRecord).length})`;
  return "N/A";
}

function JsonBlock({ title, value }: { title: string; value: unknown }) {
  const [open, setOpen] = useState(false);
  return (
    <details className="ops-json-panel" onToggle={(event) => setOpen((event.currentTarget as HTMLDetailsElement).open)}>
      <summary>
        <span>{title}</span>
        <span className="ops-chip is-small is-muted">{open ? "open" : shortValue(value)}</span>
      </summary>
      {open ? <pre>{prettyJson(value)}</pre> : null}
    </details>
  );
}

function StatusItem({ label, value }: { label: string; value: unknown }) {
  return (
    <div className="ops-status-item">
      <span>{label}</span>
      <strong>{typeof value === "boolean" ? boolText(value) : text(value)}</strong>
    </div>
  );
}

function toCandidateRow(bundle: CandidateBundle): CandidateRow {
  const summary = obj(bundle.validation_summary);
  return {
    candidateId: text(bundle.candidate_id),
    logicKey: text(bundle.logic_key),
    status: text(bundle.status),
    validationState: text(bundle.validation_state),
    createdAt: text(bundle.created_at),
    updatedAt: text(bundle.updated_at),
    readinessPass: Boolean(summary?.readiness_pass),
    sampleCount: parseNum(summary?.sample_count),
    expectancyDelta: parseNum(summary?.expectancy_delta),
    hasSnapshot: Boolean(bundle.published_ranking_snapshot),
  };
}

function ActionBanner({ level, message }: { level: "info" | "success" | "error"; message: string }) {
  return (
    <div className={`ops-alert ${level === "error" ? "is-error" : ""}`}>
      <strong>{level.toUpperCase()}</strong> {message}
    </div>
  );
}

export default function PublishOpsView() {
  const [runtimeSelection, setRuntimeSelection] = useState<RuntimeSelectionSnapshot | null>(null);
  const [publishState, setPublishState] = useState<PublishStateSnapshot | null>(null);
  const [candidateRows, setCandidateRows] = useState<CandidateRow[]>([]);
  const [candidateDetail, setCandidateDetail] = useState<CandidateBundle | null>(null);
  const [selectedLogicKey, setSelectedLogicKey] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState<string | null>(null);
  const [busyAction, setBusyAction] = useState<string | null>(null);
  const [statusMessage, setStatusMessage] = useState<{ level: "info" | "success" | "error"; message: string } | null>(null);
  const [actor, setActor] = useState("");
  const [reason, setReason] = useState("");

  const sourceOfTruth = runtimeSelection?.source_of_truth ?? publishState?.source_of_truth ?? null;
  const registrySyncState = runtimeSelection?.registry_sync_state ?? publishState?.registry_sync_state ?? null;
  const maintenanceState = runtimeSelection?.maintenance_state ?? publishState?.maintenance_state ?? null;
  const backfillRun = runtimeSelection?.candidate_backfill_last_run ?? publishState?.candidate_backfill_last_run ?? null;
  const sweepRun = runtimeSelection?.snapshot_sweep_last_run ?? publishState?.snapshot_sweep_last_run ?? null;
  const nonPromotableCount = runtimeSelection?.non_promotable_legacy_count ?? publishState?.non_promotable_legacy_count ?? null;
  const maintenanceDegraded = Boolean(runtimeSelection?.maintenance_degraded ?? publishState?.maintenance_degraded);
  const championKey = text(publishState?.champion_logic_key ?? runtimeSelection?.logic_key);
  const challengerKeys = publishState?.challenger_logic_keys ?? [];
  const selectedRow = useMemo(() => candidateRows.find((row) => row.logicKey === selectedLogicKey) ?? null, [candidateRows, selectedLogicKey]);
  const danger = Boolean(runtimeSelection?.degraded || publishState?.degraded || maintenanceDegraded);

  const loadDetail = useCallback(async (logicKey: string) => {
    setDetailLoading(true);
    setDetailError(null);
    try {
      const payload = await fetchJson<{ candidate?: CandidateBundle }>(`/api/system/publish/candidates/${encodeURIComponent(logicKey)}`);
      if (!payload.candidate) throw new Error("candidate not found");
      setCandidateDetail(payload.candidate);
    } catch (error) {
      setCandidateDetail(null);
      setDetailError(error instanceof Error ? error.message : "detail load failed");
      throw error;
    } finally {
      setDetailLoading(false);
    }
  }, []);

  const refreshAll = useCallback(
    async (detailKey: string | null = null) => {
      setRefreshing(true);
      try {
        const [runtime, publish, candidates] = await Promise.all([
          fetchJson<RuntimeSelectionSnapshot>("/api/system/runtime-selection"),
          fetchJson<PublishStateSnapshot>("/api/system/publish/state"),
          fetchJson<{ items: CandidateBundle[] }>("/api/system/publish/candidates"),
        ]);
        setRuntimeSelection(runtime);
        setPublishState(publish);
        setCandidateRows((candidates.items ?? []).map(toCandidateRow));
        if (detailKey) {
          try {
            await loadDetail(detailKey);
          } catch {
            // detailError already updated
          }
        } else {
          setCandidateDetail(null);
        }
      } catch (error) {
        setStatusMessage({ level: "error", message: error instanceof Error ? error.message : "refresh failed" });
      } finally {
        setRefreshing(false);
      }
    },
    [loadDetail]
  );

  useEffect(() => {
    void refreshAll(null);
  }, [refreshAll]);

  const runAction = useCallback(
    async (key: string, title: string, action: () => Promise<unknown>, detailKey: string | null = null) => {
      if (busyAction) return;
      setBusyAction(key);
      setStatusMessage({ level: "info", message: `${title} in progress...` });
      try {
        await action();
        setStatusMessage({ level: "success", message: `${title} finished` });
      } catch (error) {
        setStatusMessage({ level: "error", message: error instanceof Error ? error.message : `${title} failed` });
      } finally {
        setBusyAction(null);
        await refreshAll(detailKey);
      }
    },
    [busyAction, refreshAll]
  );

  const confirmRun = useCallback(
    async (key: string, title: string, message: string, action: () => Promise<unknown>, detailKey: string | null = null) => {
      if (!window.confirm(message)) return;
      await runAction(key, title, action, detailKey);
    },
    [runAction]
  );

  const candidateAction = useCallback(
    async (action: "approve" | "reject" | "promote", logicKey: string) => {
      const payload = JSON.stringify({ reason: reason || undefined, actor: actor || undefined });
      if (action === "approve") {
        await confirmRun(
          `approve:${logicKey}`,
          `Approve ${logicKey}`,
          `Approve candidate ${logicKey}?`,
          () => fetchJson(`/api/system/publish/candidates/${encodeURIComponent(logicKey)}/approve`, { method: "POST", body: payload }),
          logicKey
        );
        return;
      }
      if (action === "reject") {
        await confirmRun(
          `reject:${logicKey}`,
          `Reject ${logicKey}`,
          `Reject candidate ${logicKey}?`,
          () => fetchJson(`/api/system/publish/candidates/${encodeURIComponent(logicKey)}/reject`, { method: "POST", body: payload }),
          logicKey
        );
        return;
      }
      await confirmRun(
        `promote:${logicKey}`,
        `Promote ${logicKey}`,
        `Promote candidate ${logicKey}?`,
        () => fetchJson("/api/system/publish/promote", { method: "POST", body: JSON.stringify({ logicKey, reason: reason || undefined, actor: actor || undefined }) }),
        logicKey
      );
    },
    [actor, confirmRun, reason]
  );

  const registryAction = useCallback(
    async (action: "demote" | "rollback") => {
      const key = action === "demote" ? championKey : publishState?.previous_stable_champion_logic_key ?? championKey;
      if (!key || key === "N/A") return;
        await confirmRun(
          `${action}:${key}`,
          action === "demote" ? `Demote ${key}` : `Rollback ${key}`,
          action === "demote" ? `Demote champion ${key}?` : `Rollback to ${key}?`,
          () => fetchJson(`/api/system/publish/${action}`, { method: "POST", body: JSON.stringify({ logicKey: key, reason: reason || undefined, actor: actor || undefined }) }),
        selectedLogicKey
      );
    },
    [actor, championKey, confirmRun, publishState?.previous_stable_champion_logic_key, reason, selectedLogicKey]
  );

  const maintenanceAction = useCallback(
    async (action: "backfill" | "snapshot-sweep" | "cleanup" | "mirror-normalize" | "mirror-resync", dryRun: boolean) => {
      const key = `${action}:${dryRun ? "dry" : "run"}`;
      const label = action.replaceAll("-", " ");
      const needsConfirm = !dryRun || action === "mirror-normalize" || action === "mirror-resync";
      const endpoint =
        action === "backfill"
          ? "/api/system/publish/maintenance/backfill"
          : action === "snapshot-sweep"
            ? "/api/system/publish/maintenance/snapshot-sweep"
            : action === "cleanup"
              ? "/api/system/publish/maintenance/cleanup"
              : action === "mirror-normalize"
                ? "/api/system/publish/mirror/normalize"
                : "/api/system/publish/mirror/resync";
      const body = JSON.stringify({
        dryRun,
        keepApprovedDays: KEEP_APPROVED_DAYS,
        keepRejectedDays: KEEP_REJECTED_DAYS,
        keepRetiredDays: KEEP_RETIRED_DAYS,
        reason: reason || undefined,
        actor: actor || undefined,
      });
      const runner = () => fetchJson(endpoint, { method: "POST", body });
      if (needsConfirm) {
        await confirmRun(key, label, `Run ${label}?`, runner, selectedLogicKey);
      } else {
        await runAction(key, label, runner, selectedLogicKey);
      }
    },
    [actor, confirmRun, reason, runAction, selectedLogicKey]
  );

  const selectCandidate = useCallback(
    async (logicKey: string) => {
      setSelectedLogicKey(logicKey);
      await loadDetail(logicKey);
    },
    [loadDetail]
  );

  return (
    <div className="publish-ops-shell">
      <header className="publish-ops-header">
        <div className="publish-ops-heading">
          <div className="publish-ops-title-block">
            <div className="publish-ops-title">Operator Console</div>
            <div className="publish-ops-subtitle">MeeMee Screener publish / runtime / maintenance control surface. Operators only.</div>
          </div>
          <div className="publish-ops-actions">
            <div className="ops-field ops-field-inline ops-field-wide">
              <span>Actor</span>
              <input value={actor} onChange={(event) => setActor(event.target.value)} placeholder="operator name" />
            </div>
            <div className="ops-field ops-field-inline ops-field-wide">
              <span>Reason</span>
              <input value={reason} onChange={(event) => setReason(event.target.value)} placeholder="optional reason" />
            </div>
          </div>
        </div>

        {statusMessage ? <ActionBanner level={statusMessage.level} message={statusMessage.message} /> : null}
        {danger ? (
          <div className="ops-alert is-error">
            <strong>DEGRADED</strong> source_of_truth={text(sourceOfTruth)} / maintenance_degraded={boolText(maintenanceDegraded)}
          </div>
        ) : null}
      </header>

      <section className="publish-ops-grid">
        <article className="ops-card">
          <div className="ops-card-head">
            <div>
              <div className="ops-card-title">Runtime selection</div>
              <div className="ops-card-caption">override / last_known_good / semantic gate</div>
            </div>
            <span className={`ops-badge ${badgeClass(runtimeSelection?.source_of_truth)}`}>{text(runtimeSelection?.source_of_truth)}</span>
          </div>
          <div className="ops-status-grid">
            <StatusItem label="resolved_source" value={runtimeSelection?.resolved_source} />
            <StatusItem label="selected_logic_id" value={runtimeSelection?.selected_logic_id} />
            <StatusItem label="selected_logic_version" value={runtimeSelection?.selected_logic_version} />
            <StatusItem label="logic_key" value={runtimeSelection?.logic_key} />
            <StatusItem label="artifact_uri" value={runtimeSelection?.artifact_uri} />
            <StatusItem label="bootstrap_rule" value={runtimeSelection?.bootstrap_rule} />
          </div>
          <div className="ops-chip-row">
            <div className="ops-chip-group">
              <span className="ops-chip-label">state</span>
              <span className={`ops-chip ${badgeClass(sourceOfTruth)}`}>{text(sourceOfTruth)}</span>
              <span className={`ops-chip ${runtimeSelection?.degraded ? "is-danger" : "is-ok"}`}>{runtimeSelection?.degraded ? "degraded" : "healthy"}</span>
              <span className="ops-chip is-muted">override: {boolText(runtimeSelection?.override_present)}</span>
              <span className="ops-chip is-muted">lkg: {boolText(runtimeSelection?.last_known_good_present)}</span>
            </div>
            <div className="ops-chip-group">
              <span className="ops-chip-label">selected</span>
              <span className="ops-chip is-active">{text(runtimeSelection?.logic_key)}</span>
            </div>
          </div>
          <JsonBlock title="selected_logic_override" value={runtimeSelection?.selected_logic_override} />
          <JsonBlock title="last_known_good" value={runtimeSelection?.last_known_good} />
        </article>

        <article className="ops-card">
          <div className="ops-card-head">
            <div>
              <div className="ops-card-title">Publish registry</div>
              <div className="ops-card-caption">champion / challengers / default pointer</div>
            </div>
            <span className={`ops-badge ${badgeClass(registrySyncState)}`}>{text(registrySyncState)}</span>
          </div>
          <div className="ops-status-grid">
            <StatusItem label="champion" value={publishState?.champion_logic_key ?? championKey} />
            <StatusItem label="default_logic_pointer" value={publishState?.default_logic_pointer} />
            <StatusItem label="previous stable" value={publishState?.previous_stable_champion_logic_key} />
            <StatusItem label="last_sync_time" value={publishState?.last_sync_time ?? runtimeSelection?.last_sync_time} />
            <StatusItem label="external_registry_version" value={publishState?.external_registry_version} />
            <StatusItem label="mirror_schema_version" value={publishState?.mirror_schema_version} />
          </div>
          <div className="ops-chip-row">
            <div className="ops-chip-group">
              <span className="ops-chip-label">challengers</span>
              {(challengerKeys.length ? challengerKeys : ["N/A"]).map((key) => (
                <span key={key} className={`ops-chip ${key === selectedLogicKey ? "is-active" : "is-muted"}`}>
                  {key}
                </span>
              ))}
            </div>
            <div className="ops-chip-group">
              <span className="ops-chip-label">sync</span>
              <span className={`ops-chip ${publishState?.degraded ? "is-danger" : "is-ok"}`}>{publishState?.degraded ? "degraded" : "healthy"}</span>
              <span className="ops-chip is-muted">mirror: {boolText(publishState?.mirror_normalized)}</span>
            </div>
          </div>
          <div className="ops-detail-actions">
            <button type="button" className="ops-button" disabled={busyAction !== null || !selectedLogicKey} onClick={() => (selectedLogicKey ? void candidateAction("promote", selectedLogicKey) : undefined)}>
              Promote selected
            </button>
            <button type="button" className="ops-button" disabled={busyAction !== null || !championKey || championKey === "N/A"} onClick={() => void registryAction("demote") }>
              Demote champion
            </button>
            <button type="button" className="ops-button" disabled={busyAction !== null || (!publishState?.previous_stable_champion_logic_key && championKey === "N/A")} onClick={() => void registryAction("rollback") }>
              Rollback
            </button>
          </div>
        </article>

        <article className="ops-card">
          <div className="ops-card-head">
            <div>
              <div className="ops-card-title">Maintenance</div>
              <div className="ops-card-caption">backfill / sweep / mirror repair</div>
            </div>
            <span className={`ops-badge ${maintenanceDegraded ? "is-danger" : "is-ok"}`}>{maintenanceDegraded ? "degraded" : "ready"}</span>
          </div>
          <div className="ops-status-grid">
            <StatusItem label="candidate_backfill_last_run" value={backfillRun?.ended_at ?? backfillRun?.started_at ?? backfillRun} />
            <StatusItem label="snapshot_sweep_last_run" value={sweepRun?.ended_at ?? sweepRun?.started_at ?? sweepRun} />
            <StatusItem label="non_promotable_legacy_count" value={nonPromotableCount} />
            <StatusItem label="maintenance_degraded" value={maintenanceDegraded} />
          </div>
          <div className="ops-detail-actions">
            <button type="button" className="ops-button" disabled={busyAction !== null} onClick={() => void maintenanceAction("backfill", true)}>
              Backfill dry-run
            </button>
            <button type="button" className="ops-button" disabled={busyAction !== null} onClick={() => void maintenanceAction("backfill", false)}>
              Backfill run
            </button>
            <button type="button" className="ops-button" disabled={busyAction !== null} onClick={() => void maintenanceAction("snapshot-sweep", true)}>
              Sweep dry-run
            </button>
            <button type="button" className="ops-button" disabled={busyAction !== null} onClick={() => void maintenanceAction("snapshot-sweep", false)}>
              Sweep run
            </button>
            <button type="button" className="ops-button" disabled={busyAction !== null} onClick={() => void maintenanceAction("cleanup", true)}>
              Cleanup dry-run
            </button>
            <button type="button" className="ops-button" disabled={busyAction !== null} onClick={() => void maintenanceAction("cleanup", false)}>
              Cleanup run
            </button>
            <button type="button" className="ops-button" disabled={busyAction !== null} onClick={() => void maintenanceAction("mirror-normalize", false)}>
              Mirror normalize
            </button>
            <button type="button" className="ops-button" disabled={busyAction !== null} onClick={() => void maintenanceAction("mirror-resync", false)}>
              Mirror resync
            </button>
          </div>
        </article>
      </section>

      <section className="ops-card ops-table-card">
        <div className="ops-card-head">
          <div>
            <div className="ops-card-title">Candidate bundles</div>
            <div className="ops-card-caption">details load only when selected</div>
          </div>
          <span className={`ops-badge ${refreshing ? "is-warn" : "is-ok"}`}>{refreshing ? "refreshing" : "ready"}</span>
        </div>
        <div className="ops-table-wrap">
          <table className="ops-table">
            <thead>
              <tr>
                <th>logic_key</th>
                <th>status</th>
                <th>validation</th>
                <th>created_at</th>
                <th>updated_at</th>
                <th>readiness</th>
                <th>sample_count</th>
                <th>expectancy_delta</th>
                <th>snapshot</th>
                <th>actions</th>
              </tr>
            </thead>
            <tbody>
              {candidateRows.length ? (
                candidateRows.map((row) => {
                  const selected = row.logicKey === selectedLogicKey;
                  return (
                    <tr key={row.logicKey} className={selected ? "is-selected" : undefined} onClick={() => void selectCandidate(row.logicKey)}>
                      <td>
                        <button type="button" className="ops-chip is-small" onClick={(event) => { event.stopPropagation(); void selectCandidate(row.logicKey); }}>
                          {row.logicKey}
                        </button>
                      </td>
                      <td>
                        <span className={`ops-chip ${badgeClass(row.status)}`}>{row.status}</span>
                      </td>
                      <td>
                        <span className={`ops-chip ${badgeClass(row.validationState)}`}>{row.validationState}</span>
                      </td>
                      <td>{row.createdAt}</td>
                      <td>{row.updatedAt}</td>
                      <td>
                        <span className={`ops-chip ${row.readinessPass ? "is-ok" : "is-warn"}`}>{row.readinessPass ? "true" : "false"}</span>
                      </td>
                      <td>{numText(row.sampleCount)}</td>
                      <td>{numText(row.expectancyDelta)}</td>
                      <td>
                        <span className={`ops-chip ${row.hasSnapshot ? "is-ok" : "is-muted"}`}>{row.hasSnapshot ? "yes" : "no"}</span>
                      </td>
                      <td>
                        <div className="ops-mini-actions">
                          <button type="button" className="ops-button" onClick={(event) => { event.stopPropagation(); void selectCandidate(row.logicKey); }}>
                            Detail
                          </button>
                          <button type="button" className="ops-button" disabled={busyAction !== null} onClick={(event) => { event.stopPropagation(); void candidateAction("approve", row.logicKey); }}>
                            Approve
                          </button>
                          <button type="button" className="ops-button" disabled={busyAction !== null} onClick={(event) => { event.stopPropagation(); void candidateAction("reject", row.logicKey); }}>
                            Reject
                          </button>
                        </div>
                      </td>
                    </tr>
                  );
                })
              ) : (
                <tr>
                  <td colSpan={10}>No candidate bundles found.</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>

      <section className="publish-ops-grid">
        <article className="ops-card ops-detail-card">
          <div className="ops-card-head">
            <div>
              <div className="ops-card-title">Selected candidate detail</div>
              <div className="ops-card-caption">manifest / validation_summary / ranking snapshot</div>
            </div>
            <span className={`ops-badge ${selectedLogicKey ? "is-ok" : "is-neutral"}`}>{selectedLogicKey ?? "unselected"}</span>
          </div>

          {!selectedLogicKey ? (
            <div className="ops-alert">Select a candidate to load its detail on demand.</div>
          ) : detailLoading ? (
            <div className="ops-alert">Loading candidate detail...</div>
          ) : detailError ? (
            <div className="ops-alert is-error">{detailError}</div>
          ) : candidateDetail ? (
            <>
              <div className="ops-status-grid">
                <StatusItem label="candidate_id" value={candidateDetail.candidate_id} />
                <StatusItem label="logic_key" value={candidateDetail.logic_key} />
                <StatusItem label="status" value={candidateDetail.status} />
                <StatusItem label="validation_state" value={candidateDetail.validation_state} />
              </div>
              <div className="ops-chip-row">
                <div className="ops-chip-group">
                  <span className="ops-chip-label">ranking snapshot</span>
                  <span className={`ops-chip ${candidateDetail.published_ranking_snapshot ? "is-ok" : "is-muted"}`}>
                    {candidateDetail.published_ranking_snapshot ? "present" : "absent"}
                  </span>
                </div>
              </div>
              <div className="ops-detail-actions">
                <button type="button" className="ops-button" disabled={busyAction !== null} onClick={() => void candidateAction("promote", candidateDetail.logic_key)}>
                  Promote
                </button>
                <button type="button" className="ops-button" disabled={busyAction !== null} onClick={() => void candidateAction("approve", candidateDetail.logic_key)}>
                  Approve
                </button>
                <button type="button" className="ops-button" disabled={busyAction !== null} onClick={() => void candidateAction("reject", candidateDetail.logic_key)}>
                  Reject
                </button>
              </div>
              <JsonBlock title="published_logic_manifest" value={candidateDetail.published_logic_manifest} />
              <JsonBlock title="validation_summary" value={candidateDetail.validation_summary} />
              <JsonBlock title="published_logic_artifact" value={candidateDetail.published_logic_artifact} />
              <JsonBlock title="published_ranking_snapshot" value={candidateDetail.published_ranking_snapshot} />
            </>
          ) : (
            <div className="ops-alert is-error">Failed to load candidate detail.</div>
          )}
        </article>

        <article className="ops-card ops-detail-card">
          <div className="ops-card-head">
            <div>
              <div className="ops-card-title">Publish / maintenance state</div>
              <div className="ops-card-caption">source_of_truth / sync / maintenance</div>
            </div>
            <span className={`ops-badge ${maintenanceDegraded ? "is-danger" : "is-ok"}`}>{maintenanceDegraded ? "degraded" : "healthy"}</span>
          </div>
          <div className="ops-status-grid">
            <StatusItem label="source_of_truth" value={sourceOfTruth} />
            <StatusItem label="registry_sync_state" value={registrySyncState} />
            <StatusItem label="bootstrap_rule" value={publishState?.bootstrap_rule ?? runtimeSelection?.bootstrap_rule} />
            <StatusItem label="last_sync_time" value={publishState?.last_sync_time ?? runtimeSelection?.last_sync_time} />
            <StatusItem label="non_promotable_legacy_count" value={nonPromotableCount} />
            <StatusItem label="maintenance_degraded" value={maintenanceDegraded} />
          </div>
          <JsonBlock title="maintenance_state" value={maintenanceState} />
          <JsonBlock title="publish_registry_state" value={publishState} />
          <JsonBlock title="runtime_selection_snapshot" value={runtimeSelection} />
        </article>
      </section>

      {selectedRow ? <div className="ops-alert">Selected row: {selectedRow.logicKey} / status={selectedRow.status} / readiness={selectedRow.readinessPass ? "true" : "false"}</div> : null}
    </div>
  );
}
