param(
  [string]$DbPath = "C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb",
  [string]$OutputRoot = "G:\Tradex\tradex_long_fresh_operational_board_v1",
  [string]$EntryLedger = "G:\Tradex\tradex_long_fresh_operational_board_v1\state\entry_ledger.json",
  [string]$AdoptionAudit = "G:\Tradex\tradex_long_fresh_adoption_audit_v1\20260720T-authoritative-v2\audit.json",
  [string]$TailExitCompare = "G:\Tradex\tradex_long_fresh_tail_guard_day5_exit_v1\20260720T-authoritative-v2\compare.json",
  [string]$ChartReview
)

$ErrorActionPreference = "Stop"
$tag = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
$output = Join-Path $OutputRoot "$tag-operational"
$arguments = @(
  "scripts\tradex_long_fresh_operational_board_v1.py",
  "--output", $output,
  "--db", $DbPath,
  "--adoption-audit", $AdoptionAudit,
  "--tail-exit-compare", $TailExitCompare
)
if ($ChartReview) { $arguments += @("--chart-review", $ChartReview) }
if (Test-Path -LiteralPath $EntryLedger) { $arguments += @("--entry-ledger", $EntryLedger) }

& python @arguments
if ($LASTEXITCODE -ne 0) { throw "TRADEX long operational board generation failed with exit code $LASTEXITCODE" }
$artifact = Join-Path $output "operational_board.json"
$payload = Get-Content -LiteralPath $artifact -Raw -Encoding UTF8 | ConvertFrom-Json
if ($payload.judgment.authoritative_rollup_decision -ne "PRODUCTION_DECISION_SUPPORT_READY") {
  throw "TRADEX long operational board stopped: $($payload.judgment.authoritative_rollup_decision); inspect $artifact"
}
$pointer = [ordered]@{
  schema_version = "tradex_long_fresh_operational_latest_pointer_v1"
  generated_at = (Get-Date).ToUniversalTime().ToString("o")
  run_root = $output
  authoritative_artifact = $artifact
  latest_as_of = $payload.latest_as_of
  decision = $payload.judgment.authoritative_rollup_decision
}
$pointer | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $OutputRoot "latest_operational_board.json") -Encoding utf8
Write-Output $artifact
