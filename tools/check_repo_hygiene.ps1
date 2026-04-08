param(
    [string]$RepoRoot = ""
)

$ErrorActionPreference = "Stop"

if (-not $RepoRoot) {
    $RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
}

Set-Location $RepoRoot

$scratchNames = @(
    ".pytest-bt",
    ".pytest_cache",
    ".tmp-pytest",
    ".tmp-pytest-fixtures",
    ".tmp-pytest-root",
    ".tmp-python",
    ".tmp-test",
    ".tmp-test-tmp",
    ".tmp-tests",
    "tmp",
    "_pytest_work"
)

$scratchHits = foreach ($name in $scratchNames) {
    if (Test-Path -LiteralPath $name) {
        $fileCount = 0
        try {
            $fileCount = (Get-ChildItem -LiteralPath $name -Force -File -Recurse -ErrorAction SilentlyContinue | Measure-Object).Count
        } catch {
            $fileCount = 0
        }
        [pscustomobject]@{
            Name = $name
            Files = $fileCount
        }
    }
}

if ($scratchHits) {
    Write-Host "Repo hygiene check failed: repo-root scratch/cache trees found." -ForegroundColor Red
    $scratchHits | Sort-Object Name | Format-Table -AutoSize | Out-Host
    exit 1
}

$statusWatch = [System.Diagnostics.Stopwatch]::StartNew()
$statusLines = @()
try {
    $statusLines = git status --porcelain=v1 --untracked-files=normal
} finally {
    $statusWatch.Stop()
}

$statusCount = @($statusLines).Count
Write-Host ("git status entries: {0}" -f $statusCount)
Write-Host ("git status elapsed: {0:n2}s" -f $statusWatch.Elapsed.TotalSeconds)

if ($statusWatch.Elapsed.TotalSeconds -gt 10) {
    Write-Host "Repo hygiene warning: git status is slow." -ForegroundColor Yellow
    exit 2
}

if ($statusCount -gt 200) {
    Write-Host "Repo hygiene warning: git status has a large entry count." -ForegroundColor Yellow
    exit 2
}

Write-Host "Repo hygiene check passed." -ForegroundColor Green
