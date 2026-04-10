param(
    [string]$RepoRoot = ""
)

$ErrorActionPreference = "Stop"

if (-not $RepoRoot) {
    $RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
}

Set-Location $RepoRoot

function Get-RelativeRepoPath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$FullPath
    )

    $relative = $FullPath.Substring($RepoRoot.Length).TrimStart('\', '/')
    return $relative -replace '\\', '/'
}

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

$scratchHits += Get-ChildItem -Force -Directory -ErrorAction SilentlyContinue | Where-Object {
    $_.Name -like "pytest-cache-files-*"
} | ForEach-Object {
    $fileCount = 0
    try {
        $fileCount = (Get-ChildItem -LiteralPath $_.FullName -Force -File -Recurse -ErrorAction SilentlyContinue | Measure-Object).Count
    } catch {
        $fileCount = 0
    }
    if ($fileCount -gt 0) {
        [pscustomobject]@{
            Name = $_.Name
            Files = $fileCount
        }
    }
}

if ($scratchHits) {
    Write-Host "Repo hygiene check failed: repo-root scratch/cache trees found." -ForegroundColor Red
    $scratchHits | Sort-Object Name | Format-Table -AutoSize | Out-Host
    exit 1
}

$residentArtifactViolations = @()

$buildRoot = Join-Path $RepoRoot "build"
if (Test-Path -LiteralPath $buildRoot) {
    $allowedBuildFiles = @(
        "build/pyinstaller/MeeMeeScreener.spec"
    )
    $unexpectedBuildFiles = @(Get-ChildItem -LiteralPath $buildRoot -Force -File -Recurse -ErrorAction SilentlyContinue | Where-Object {
        (Get-RelativeRepoPath $_.FullName) -notin $allowedBuildFiles
    })
    if ($unexpectedBuildFiles.Count -gt 0) {
        $residentArtifactViolations += [pscustomobject]@{
            Kind = "build"
            Path = "build"
            Files = $unexpectedBuildFiles.Count
            SizeMB = [math]::Round((($unexpectedBuildFiles | Measure-Object Length -Sum).Sum / 1MB), 2)
            Detail = "Build artifacts must not stay resident in the repo."
        }
    }
}

$releaseRoot = Join-Path $RepoRoot "release"
if (Test-Path -LiteralPath $releaseRoot) {
    $unexpectedReleaseFiles = @(Get-ChildItem -LiteralPath $releaseRoot -Force -File -Recurse -ErrorAction SilentlyContinue | Where-Object {
        (Get-RelativeRepoPath $_.FullName) -notmatch '^release/[^/]+\.zip$'
    })
    if ($unexpectedReleaseFiles.Count -gt 0) {
        $residentArtifactViolations += [pscustomobject]@{
            Kind = "release"
            Path = "release"
            Files = $unexpectedReleaseFiles.Count
            SizeMB = [math]::Round((($unexpectedReleaseFiles | Measure-Object Length -Sum).Sum / 1MB), 2)
            Detail = "Only portable zip artifacts may remain under release/."
        }
    }
}

if ($residentArtifactViolations) {
    Write-Host "Repo hygiene check failed: resident build/release artifacts found." -ForegroundColor Red
    $residentArtifactViolations | Sort-Object Kind, Path | Format-Table -AutoSize | Out-Host
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
