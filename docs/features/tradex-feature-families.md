# TRADEX Feature Families

Every candidate entering compare must declare `feature_family`.

Allowed values:

- `environment_recognition`
- `common_pattern`
- `regime_adjustment`
- `boundary_feature`
- `bad_pick_removal`
- `symbol_specific_adjustment`
- `image_context_support`

## Rules

- Candidates without `feature_family` must not enter compare.
- The field is contract data, not markdown-only metadata.
- The field must be preserved through compare, leaderboard, and rollup artifacts.

## Intent

Feature families make the harness explain why a candidate exists.
They are used to separate environment modeling, generic patterns, regime adaptation, boundary handling, bad-pick removal, symbol-specific tuning, and image-assisted support.
