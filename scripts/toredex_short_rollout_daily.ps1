param(
  [string]$SeasonId = "toredex_live_short_hybrid_prod_20260304",
  [ValidateSet("champion", "challenger")]
  [string]$OperatingMode = "champion",
  [string]$AsOf = "",
  [string]$ConfigOverrideJson = "tmp/toredex_short_hybrid_shadow_config_20260304.json",
  [int]$WindowDays = 10,
  [double]$RollbackMaxDd = -8.0,
  [string]$DailyLogPath = "",
  [switch]$AppendLog,
  [switch]$DryRun,
  [switch]$MonitorOnly,
  [switch]$FailOnRollback
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$args = @(
  "scripts/toredex_short_rollout_daily.py",
  "--season-id", $SeasonId,
  "--operating-mode", $OperatingMode,
  "--window-days", "$WindowDays",
  "--rollback-max-dd", "$RollbackMaxDd"
)

if ($AsOf -and $AsOf.Trim().Length -gt 0) {
  $args += @("--asof", $AsOf.Trim())
}
if ($ConfigOverrideJson -and $ConfigOverrideJson.Trim().Length -gt 0) {
  $args += @("--config-override-json", $ConfigOverrideJson.Trim())
}
if ($DailyLogPath -and $DailyLogPath.Trim().Length -gt 0) {
  $args += @("--daily-log-path", $DailyLogPath.Trim())
}
if ($AppendLog) { $args += "--append-log" }
if ($DryRun) { $args += "--dry-run" }
if ($MonitorOnly) { $args += "--monitor-only" }
if ($FailOnRollback) { $args += "--fail-on-rollback" }

python @args
