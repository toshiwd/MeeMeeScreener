import { useEffect, useMemo, useState } from "react";
import { api } from "../api";

type AnyRecord = Record<string, unknown>;

type TradexShadowRuntimeSnapshot = {
  available: boolean;
  loaded: boolean;
  error: string | null;
  shadowOnly: boolean | null;
  acceptanceState: string | null;
  adoptionReadiness: string | null;
  compareMethod: string | null;
  outsideTop20Locked: boolean | null;
  validationIssues: string[];
};

const text = (value: unknown): string | null => {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed.length ? trimmed : null;
};

const boolish = (value: unknown): boolean | null => {
  if (typeof value === "boolean") return value;
  if (typeof value === "number" && Number.isFinite(value)) return value !== 0;
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (["1", "true", "yes", "on"].includes(normalized)) return true;
    if (["0", "false", "no", "off"].includes(normalized)) return false;
  }
  return null;
};

const listOfText = (value: unknown): string[] => {
  if (!Array.isArray(value)) return [];
  return value
    .map((item) => text(item))
    .filter((item): item is string => item != null);
};

const isRecord = (value: unknown): value is AnyRecord =>
  Boolean(value) && typeof value === "object" && !Array.isArray(value);

const normalizeShadowRuntimeSnapshot = (payload: unknown): TradexShadowRuntimeSnapshot => {
  const root = isRecord(payload) ? payload : {};
  const shadowIntegration = isRecord(root.shadow_integration) ? root.shadow_integration : {};
  const state = isRecord(root.shadow_integration_state)
    ? root.shadow_integration_state
    : isRecord(shadowIntegration.state)
      ? shadowIntegration.state
      : {};
  const rolloutBoundary = isRecord(root.shadow_rollout_boundary)
    ? root.shadow_rollout_boundary
    : isRecord(shadowIntegration.rollout_boundary)
      ? shadowIntegration.rollout_boundary
      : {};
  const available = boolish(root.shadow_integration_available ?? shadowIntegration.available);
  const issues = [
    ...listOfText(root.shadow_integration_validation_issues),
    ...listOfText(shadowIntegration.validation_issues),
  ];
  return {
    available: available ?? false,
    loaded: true,
    error: null,
    shadowOnly: boolish(root.shadow_only ?? state.shadow_only ?? shadowIntegration.shadow_only),
    acceptanceState:
      text(root.acceptance_state) ?? text(state.acceptance_state) ?? text(shadowIntegration.acceptance_state),
    adoptionReadiness:
      text(root.adoption_readiness) ??
      text(state.adoption_readiness) ??
      text(shadowIntegration.adoption_readiness),
    compareMethod:
      text(root.compare_method) ?? text(state.compare_method) ?? text(shadowIntegration.compare_method),
    outsideTop20Locked:
      boolish(root.outside_top20_locked) ??
      boolish(state.outside_top20_locked) ??
      boolish(rolloutBoundary.outside_top20_locked),
    validationIssues: issues,
  };
};

export function useTradexShadowIntegration() {
  const [snapshot, setSnapshot] = useState<TradexShadowRuntimeSnapshot>({
    available: false,
    loaded: false,
    error: null,
    shadowOnly: null,
    acceptanceState: null,
    adoptionReadiness: null,
    compareMethod: null,
    outsideTop20Locked: null,
    validationIssues: [],
  });

  useEffect(() => {
    let cancelled = false;
    void api
      .get("/system/runtime-selection")
      .then((response) => {
        if (cancelled) return;
        setSnapshot(normalizeShadowRuntimeSnapshot(response.data));
      })
      .catch((error: unknown) => {
        if (cancelled) return;
        setSnapshot({
          available: false,
          loaded: true,
          error: error instanceof Error ? error.message : "runtime selection unavailable",
          shadowOnly: null,
          acceptanceState: null,
          adoptionReadiness: null,
          compareMethod: null,
          outsideTop20Locked: null,
          validationIssues: [],
        });
      });
    return () => {
      cancelled = true;
    };
  }, []);

  return useMemo(() => {
    const isReady = snapshot.loaded && snapshot.available && snapshot.shadowOnly === true;
    const unavailable = snapshot.loaded && (!snapshot.available || snapshot.shadowOnly !== true);
    return {
      ...snapshot,
      isReady,
      unavailable,
    };
  }, [snapshot]);
}

export type TradexShadowIntegrationState = ReturnType<typeof useTradexShadowIntegration>;
