param(
  [string]$Root = ""
)

function Resolve-Root {
  if ($Root -and (Test-Path $Root)) { return (Resolve-Path $Root).Path }
  try {
    $gitRoot = (git rev-parse --show-toplevel).Trim()
    if ($gitRoot) { return $gitRoot }
  } catch {}
  return (Get-Location).Path
}

$rootPath = Resolve-Root
$errors = @()

function Check($label, [bool]$ok, $detail) {
  if ($ok) {
    Write-Host "[ok] $label"
  } else {
    Write-Host "[ng] $label - $detail"
    $script:errors += "${label}: $detail"
  }
}

# 1) seed DB presence
$seedDb = Join-Path $rootPath 'app/backend/stocks.duckdb'
Check 'seed db exists' (Test-Path $seedDb) $seedDb

# 2) backend import
try {
  $proc = Start-Process -FilePath 'python' -ArgumentList @('-c', '"import app.backend.main"') -WorkingDirectory $rootPath -NoNewWindow -Wait -PassThru
  Check 'backend import' ($proc.ExitCode -eq 0) "exit=$($proc.ExitCode)"
} catch {
  Check 'backend import' $false $_.Exception.Message
}

# 3) frontend static exists
$staticDir = Join-Path $rootPath 'app/backend/static'
$indexFile = Join-Path $staticDir 'index.html'
Check 'frontend static dir exists' (Test-Path $staticDir) $staticDir
Check 'frontend index.html exists' (Test-Path $indexFile) $indexFile

if ($errors.Count -gt 0) {
  Write-Host '---'
  Write-Host 'Smoke test failed:'
  $errors | ForEach-Object { Write-Host " - $_" }
  exit 1
}

Write-Host 'Smoke test OK.'
