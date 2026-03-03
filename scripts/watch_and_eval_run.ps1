param(
  [Parameter(Mandatory = $true)]
  [string]$RunId,
  [int]$PollSeconds = 15,
  [int]$MaxWaitMinutes = 180,
  [switch]$RunAnalysis
)

$ErrorActionPreference = "Stop"
$repoRoot = "c:\work\meemee-screener"
$runDir = Join-Path $repoRoot "research_workspace\runs\$RunId"
$manifest = Join-Path $runDir "manifest.json"
$deadline = (Get-Date).AddMinutes($MaxWaitMinutes)

Write-Output "[watch] run_id=$RunId poll=${PollSeconds}s max_wait=${MaxWaitMinutes}m"
while ((Get-Date) -lt $deadline) {
  if (Test-Path $manifest) {
    Write-Output "[watch] manifest detected: $manifest"
    break
  }
  Start-Sleep -Seconds $PollSeconds
}

if (-not (Test-Path $manifest)) {
  throw "timeout waiting run manifest: $manifest"
}

Push-Location $repoRoot
try {
  Write-Output "[watch] evaluate start"
  python -m research evaluate --run_id $RunId
  if ($LASTEXITCODE -ne 0) { throw "evaluate failed: run_id=$RunId" }

  if ($RunAnalysis) {
    Write-Output "[watch] analysis start"
    python research/execute_all_analysis.py --run-id $RunId
    if ($LASTEXITCODE -ne 0) { throw "analysis failed: run_id=$RunId" }
  }
  Write-Output "[watch] done"
}
finally {
  Pop-Location
}

