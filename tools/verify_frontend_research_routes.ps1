param(
    [Parameter(Mandatory = $true)]
    [string[]]$TargetPaths
)

$ErrorActionPreference = "Stop"

$forbiddenRoute = "/analysis-bridge/internal/"
$requiredRoute = "/tradex/research"
$scanExtensions = @(".js", ".mjs", ".cjs", ".css", ".html", ".json", ".map", ".txt")
$violations = @()
$requiredHits = 0

foreach ($targetPath in $TargetPaths) {
    if ([string]::IsNullOrWhiteSpace($targetPath)) {
        continue
    }

    if (-not (Test-Path -LiteralPath $targetPath)) {
        throw "Frontend route verification target not found: $targetPath"
    }

    $resolvedTarget = (Resolve-Path -LiteralPath $targetPath).Path
    $files = Get-ChildItem -LiteralPath $resolvedTarget -File -Recurse -ErrorAction SilentlyContinue | Where-Object {
        $_.Extension -in $scanExtensions
    }

    foreach ($file in $files) {
        $content = Get-Content -LiteralPath $file.FullName -Raw -ErrorAction SilentlyContinue
        if ([string]::IsNullOrEmpty($content)) {
            continue
        }
        if ($content.Contains($forbiddenRoute)) {
            $violations += $file.FullName
        }
        if ($content.Contains($requiredRoute)) {
            $requiredHits += 1
        }
    }
}

if ($violations.Count -gt 0) {
    Write-Host "Frontend route verification failed: legacy analysis_bridge internal routes remain in generated assets." -ForegroundColor Red
    $violations | Sort-Object -Unique | ForEach-Object { Write-Host " - $_" }
    exit 1
}

if ($requiredHits -le 0) {
    Write-Host "Frontend route verification failed: generated assets do not reference tradex research routes." -ForegroundColor Red
    exit 1
}

Write-Host "Frontend route verification passed." -ForegroundColor Green
Write-Host "Targets: $($TargetPaths -join ', ')"
Write-Host "Files referencing ${requiredRoute}: $requiredHits"
