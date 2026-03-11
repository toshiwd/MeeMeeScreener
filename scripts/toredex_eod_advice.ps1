param(
  [string]$Symbols = "",
  [string]$AsOf = "",
  [ValidateSet("defensive", "balanced", "aggressive")]
  [string]$RiskProfile = "balanced",
  [string]$SeasonId = "toredex_live_short_hybrid_prod_20260304",
  [int]$LotSize = 100,
  [string]$Output = "",
  [switch]$YahooVerify
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$args = @(
  "scripts/toredex_eod_advisor.py",
  "--risk-profile", $RiskProfile,
  "--season-id", $SeasonId,
  "--lot-size", "$LotSize"
)

if ($Symbols -and $Symbols.Trim().Length -gt 0) {
  $args += @("--symbols", $Symbols.Trim())
}
if ($AsOf -and $AsOf.Trim().Length -gt 0) {
  $args += @("--as-of", $AsOf.Trim())
}
if ($Output -and $Output.Trim().Length -gt 0) {
  $args += @("--output", $Output.Trim())
}
if ($YahooVerify) {
  $args += "--yahoo-verify"
}

python @args
