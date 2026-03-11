param(
  [int]$Phase = 0,
  [string]$AsOfDate = "",
  [string]$PlanFile = "tmp/monthly_execution_plan_202603_strictplus_backups.json",
  [string]$BasePlanFile = "tmp/monthly_execution_plan_202603.json",
  [string]$OutputDir = "tmp",
  [string]$PythonExe = "python"
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

if (-not $AsOfDate -or $AsOfDate.Trim().Length -eq 0) {
  $AsOfDate = (Get-Date).ToString("yyyy-MM-dd")
}

$asofCompact = $AsOfDate.Replace("-", "")
$phaseToken = if ($Phase -gt 0) { "phase$Phase" } else { "auto" }
$jsonOut = Join-Path $OutputDir ("monthly_open_check_{0}_{1}.json" -f $asofCompact, $phaseToken)
$csvOut = Join-Path $OutputDir ("monthly_open_selected_{0}_{1}.csv" -f $asofCompact, $phaseToken)

$args = @(
  "scripts/monthly_open_recheck.py",
  "--plan-file", $PlanFile,
  "--base-plan-file", $BasePlanFile,
  "--asof-date", $AsOfDate,
  "--output", $jsonOut,
  "--output-csv", $csvOut
)
if ($Phase -gt 0) {
  $args += @("--phase", "$Phase")
}

Write-Host "Running monthly pre-open recheck..."
$raw = & $PythonExe @args
if ($LASTEXITCODE -ne 0) {
  throw "monthly_open_recheck.py failed"
}

$text = ($raw | Out-String).Trim()
if (-not $text) {
  throw "monthly_open_recheck.py returned empty output"
}
$obj = $text | ConvertFrom-Json
if (-not $obj.ok) {
  throw "monthly_open_recheck.py returned non-ok response"
}

Write-Host ("phase={0} phase_date={1}" -f $obj.phase, $obj.phase_date)
Write-Host ("summary kept={0} replaced={1} vacant={2}" -f $obj.summary.kept_primary, $obj.summary.replaced_primary, $obj.summary.vacant_slots)
Write-Host ("json={0}" -f $obj.output_json)
Write-Host ("csv={0}" -f $obj.output_csv)
