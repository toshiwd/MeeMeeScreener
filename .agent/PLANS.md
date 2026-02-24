# Codex Execution Plans (ExecPlans)

This document defines the required structure for an execution plan ("ExecPlan"): a design document that a coding agent can follow to deliver a working feature or system change.

Treat the reader as a complete beginner to this repository. They only have:
- the current working tree
- the single ExecPlan file you provide

Assume no memory of prior plans and no external context.

## How to Use ExecPlans and This File

When authoring an executable specification (ExecPlan), follow this file to the letter.

If this file is not in your active context, re-read the entire `.agent/PLANS.md` before writing or revising a plan. Be thorough when reading source material so the specification stays accurate.

When creating a spec, start from the skeleton in this document and fill it in while researching.

When implementing an ExecPlan:
- do not ask the user for "next steps"; proceed to the next milestone
- keep all sections up to date
- add or split Progress entries at every stopping point so status is explicit
- resolve ambiguities autonomously
- commit frequently

When discussing an ExecPlan, record decisions in the Decision Log so it is always clear why the plan changed.

ExecPlans are living documents. It must be possible to restart from only the ExecPlan and no other work.

For difficult designs or unknowns, include milestones for proofs of concept ("toy implementations") that test feasibility early.

Do deep technical research when needed. Read dependency source code if necessary. Include prototypes that guide full implementation.

## Requirements

NON-NEGOTIABLE REQUIREMENTS:
- Every ExecPlan must be fully self-contained.
- Every ExecPlan is a living document and must be revised as progress, discoveries, and decisions happen.
- Every ExecPlan must enable a complete novice to implement the feature end-to-end.
- Every ExecPlan must produce demonstrably working behavior, not just code edits.
- Every ExecPlan must define every term of art in plain language (or avoid the term).

Begin with purpose and user value first. In a few sentences, explain:
- why the work matters from a user perspective
- what users can do after the change that they could not do before
- how to observe it working

Then guide the reader through exact steps:
- what to edit
- what commands to run
- what they should observe

Assume the executing agent can:
- list files
- read files
- search
- run the project
- run tests

Repeat any assumption you rely on.

Do not rely on external blogs/docs. If knowledge is required, embed it in the plan in your own words.

If an ExecPlan builds on a prior checked-in ExecPlan, reference that file. If not checked in, include all relevant context directly.

## Formatting

Format is strict:
- Each ExecPlan must be one fenced code block labeled `md` that starts and ends with triple backticks.
- Do not nest triple-backtick fences inside the ExecPlan.
- For commands, transcripts, diffs, and code examples inside the ExecPlan, use indented blocks.
- Use two newlines after every heading.
- Use proper `#`, `##`, etc.
- Use correct ordered/unordered list syntax.

When writing an ExecPlan into a `.md` file whose content is only that plan, omit the outer triple backticks.

Write plain prose. Prefer sentences over lists.

Avoid checklists, tables, and long enumerations unless brevity would become unclear.

Checklists are allowed only in the Progress section, where they are mandatory.

Narrative sections must stay prose-first.

## Guidelines

Self-containment and plain language are mandatory.

If you use non-obvious terms (for example: "daemon", "middleware", "RPC gateway", "filter graph"), define them immediately and map them to this repo with concrete file paths/commands.

Do not say "as defined previously" or "according to the architecture doc." Include needed explanation again, even if repetitive.

Avoid common failure modes:
- undefined jargon
- narrow "letter-only" implementations that compile but do nothing useful
- outsourcing key decisions to the reader

When ambiguity exists, resolve it in the plan and explain why.

Favor:
- over-explaining user-visible effects
- under-specifying incidental implementation details

Anchor the plan with observable outcomes.

State:
- what the user can do after implementation
- exact commands to run
- expected output/behavior

Phrase acceptance as behavior a human can verify.

Example:
- "After starting the server, navigating to `http://localhost:8080/health` returns HTTP 200 with body `OK`."

Avoid acceptance phrased only as internal structure (for example, "added a `HealthCheck` struct").

If a change is internal, explain how to demonstrate impact via tests and a concrete scenario.

Specify repo context explicitly:
- full repository-relative file paths
- exact functions/modules
- where new files must be created

When touching multiple areas, include a short orientation paragraph explaining how they connect.

For commands, include:
- exact working directory
- exact command line
- assumptions/environment dependencies
- alternatives when reasonable

Be idempotent and safe:
- steps should be re-runnable without damage/drift
- include retry guidance for partial failures
- include backup/rollback guidance for risky operations
- prefer additive, testable changes

Validation is required:
- include test commands
- include startup/exercise commands if applicable
- include expected output and common error signals
- include proof beyond compilation (end-to-end flow, CLI call, or HTTP transcript)

Capture evidence:
- include concise terminal output/diffs/log snippets
- keep only evidence needed to prove success

If including patches, prefer file-scoped diffs or small excerpts that are easy to recreate.

## Milestones

Milestones are narrative, not bureaucracy.

If you split into milestones, each milestone must state:
- scope
- what will newly exist after completion
- commands to run
- acceptance signals

Write milestones as a story: goal, work, result, proof.

Progress and milestones are distinct:
- milestones explain narrative flow
- Progress tracks granular completion state

Both must exist.

Do not abbreviate milestone descriptions so much that crucial implementation details are lost.

Each milestone must be independently verifiable and incrementally contribute to the final goal.

## Living Plans and Design Decisions

ExecPlans are living documents.

As implementation evolves, update the plan with key decisions and reasoning.

Every ExecPlan must include and maintain:
- `Progress`
- `Surprises & Discoveries`
- `Decision Log`
- `Outcomes & Retrospective`

These sections are mandatory.

When you discover optimizer behavior, performance tradeoffs, bugs, or inverse/unapply semantics, record them in `Surprises & Discoveries` with short evidence snippets (test output preferred).

If you change course mid-implementation:
- document why in `Decision Log`
- reflect impacts in `Progress`

At major milestones or completion, add an `Outcomes & Retrospective` entry:
- what was achieved
- what remains
- lessons learned

## Prototyping Milestones and Parallel Implementations

Prototyping milestones are encouraged when they de-risk larger changes.

Examples:
- add a low-level operator in a dependency to validate feasibility
- evaluate two composition orders and compare optimizer behavior

Requirements for prototypes:
- clearly label as "prototyping"
- keep additive and testable
- describe how to run and observe
- define promotion/discard criteria

Parallel implementations (temporary old/new paths) are acceptable during migration when they reduce risk.

If used, define:
- how to validate both paths
- how to retire the old path safely
- tests required to keep confidence

When introducing multiple new libraries/feature areas, consider independent spikes first to prove each dependency in isolation.

## Skeleton of a Good ExecPlan

```md
# <Short, action-oriented description>

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

If `.agent/PLANS.md` exists in this repository, this ExecPlan must be maintained in accordance with it.

## Purpose / Big Picture

Explain in a few sentences what someone gains after this change and how they can see it working. State the user-visible behavior you will enable.

## Progress

Use a list with checkboxes to summarize granular steps. Every stopping point must be documented here, even if it requires splitting a partially completed task into two ("done" vs. "remaining"). This section must always reflect the actual current state of the work.

- [x] (2025-10-01 13:00Z) Example completed step.
- [ ] Example incomplete step.
- [ ] Example partially completed step (completed: X; remaining: Y).

Use timestamps to measure rates of progress.

## Surprises & Discoveries

Document unexpected behaviors, bugs, optimizations, or insights discovered during implementation. Provide concise evidence.

- Observation: ...
  Evidence: ...

## Decision Log

Record every decision made while working on the plan in the format:

- Decision: ...
  Rationale: ...
  Date/Author: ...

## Outcomes & Retrospective

Summarize outcomes, gaps, and lessons learned at major milestones or at completion. Compare the result against the original purpose.

## Context and Orientation

Describe the current state relevant to this task as if the reader knows nothing. Name the key files and modules by full path. Define any non-obvious term you will use. Do not refer to prior plans.

## Plan of Work

Describe, in prose, the sequence of edits and additions. For each edit, name the file and location (function, module) and what to insert or change. Keep it concrete and minimal.

## Concrete Steps

State the exact commands to run and where to run them (working directory). When a command generates output, show a short expected transcript so the reader can compare. This section must be updated as work proceeds.

## Validation and Acceptance

Describe how to start or exercise the system and what to observe. Phrase acceptance as behavior, with specific inputs and outputs. If tests are involved, say:

"run <project test command> and expect <N> passed; the new test <name> fails before the change and passes after"

## Idempotence and Recovery

If steps can be repeated safely, say so. If a step is risky, provide a safe retry or rollback path. Keep the environment clean after completion.

## Artifacts and Notes

Include the most important transcripts, diffs, or snippets as indented examples. Keep them concise and focused on what proves success.

## Interfaces and Dependencies

Be prescriptive. Name the libraries, modules, and services to use and why. Specify the types, traits/interfaces, and function signatures that must exist at the end of the milestone.

Example:

    In crates/foo/planner.rs, define:
    pub trait Planner {
        fn plan(&self, observed: &Observed) -> Vec<Action>;
    }

If you follow this guidance, a single stateless agent or human novice can read your ExecPlan top-to-bottom and produce a working, observable result. That is the standard: self-contained, self-sufficient, novice-guiding, and outcome-focused.

When revising a plan, reflect updates across all sections and add a note at the end describing what changed and why.
```
