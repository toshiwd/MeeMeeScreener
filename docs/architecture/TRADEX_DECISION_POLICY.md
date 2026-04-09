# TRADEX Decision Policy

This document fixes the Phase 2A authoritative decision policy for TRADEX Research OS.

## Scope

- The policy applies only to the additive TRADEX Research OS layer.
- It does not rewrite the existing single-session TRADEX runner.
- It does not add new research families, new score systems, or multi-session orchestration.

## Source Of Truth

- Family-level `compare.json` is the only compare-contract truth used for judge and authoritative decision input.
- Session-level `compare.json` is session state only and must not be treated as compare truth.
- `judge_decision.json` remains the provisional OS-layer decision record.
- `authoritative_decision.json` is the Phase 2A authoritative decision artifact.

## Decision Inputs

The authoritative decision uses only existing TRADEX artifact fields and normalized judge input fields:

- `changed_top5_members_count`
- `changed_top10_members_count`
- `changed_rank_count`
- `top5_boundary_score_gap`
- `top10_boundary_score_gap`
- `selection_divergence_reason`
- `available_sample_count`
- `available_session_count`
- existing blocker and decision fields from authoritative artifacts

The authoritative context is derived from family-level compare artifacts and family leaderboard rows. No new scoring system is introduced.

## Decision Order

The policy is evaluated in this order:

1. hard blocker exists -> `drop`
2. insufficient sample -> `hold`
3. no branching -> `hold`
4. branching exists, no blocker, and family compare is affirmative -> `keep`
5. branching exists and family compare is negative -> `drop`
6. branching exists and evaluation is weak or unclear -> `hold`
7. missing or ambiguous metrics -> `hold`

Machine-readable rule ids in `decision_policy_v1.json`:

- `hard_blocker_drop`
- `insufficient_sample_hold`
- `no_branching_hold`
- `affirmative_family_compare_keep`
- `negative_family_compare_drop`
- `weak_or_unclear_evaluation_hold`
- `missing_or_ambiguous_metrics_hold`

## Interpretation Rules

- Hard blockers are only structural or lineage-level blockers that make the evidence unusable for an authoritative decision.
- Insufficient sample means the available sample count is zero or the authoritative evidence marks the sample set as insufficient.
- No branching means the existing artifact evidence does not show meaningful top-k branching.
- Family compare affirmative means the family-level authoritative evidence is positive under the existing TRADEX compare artifact fields.
- Family compare negative means the family-level authoritative evidence is negative under the existing TRADEX compare artifact fields.
- If evidence is missing or ambiguous, the policy must prefer `hold`.

## Provisional Versus Authoritative Records

- `judge_decision.json` keeps the provisional Phase 1 decision record.
- `authoritative_decision.json` records the Phase 2A authoritative decision.
- `research_memory.json.latest_decision` must point to the authoritative decision.
- `research_memory.json.decision_history` may retain both provisional and authoritative entries for the same experiment.

## Auditability

The authoritative decision must preserve:

- the policy version used
- the normalized decision inputs
- the blocking reasons that led to a `drop` or `hold`
- the evidence summary that explains why the policy reached the final result

This policy is intentionally minimal. Phase 2B can refine the evidence model later, but Phase 2A fixes the decision contract now.
