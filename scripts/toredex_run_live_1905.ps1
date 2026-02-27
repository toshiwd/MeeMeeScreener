param(
  [Parameter(Mandatory = $true)]
  [string]$SeasonId,

  [ValidateSet("champion", "challenger")]
  [string]$OperatingMode = "champion",

  [string]$AsOf = ""
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$args = @(
  "-m", "toredex", "run-live",
  "--season-id", $SeasonId,
  "--operating-mode", $OperatingMode
)
if ($AsOf -and $AsOf.Trim().Length -gt 0) {
  $args += @("--asof", $AsOf.Trim())
}

python @args
