param(
    [string]$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path,
    [string]$PythonExe = "python",
    [string]$Phase3Dir = "G:\Tradex\meemee_multiscale_dataset_scale_phase3\20260601T080600Z-meemee_multiscale_dataset_scale_phase3",
    [string]$ExportRoot = "G:\Tradex\meemee_canonical_export_phase4",
    [string]$DbPath = "C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb",
    [int]$BatchSize = 256,
    [int]$MaxBatches = 1000,
    [int]$TimeoutSeconds = 300
)

$ErrorActionPreference = "Stop"
$opsRoot = Join-Path $ExportRoot "ops"
New-Item -ItemType Directory -Path $opsRoot -Force | Out-Null
$tag = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssfffZ")
$stdoutPath = Join-Path $opsRoot "$tag-phase6.stdout.log"
$stderrPath = Join-Path $opsRoot "$tag-phase6.stderr.log"
$launchPath = Join-Path $opsRoot "$tag-phase6.launch.json"
$latestPath = Join-Path $opsRoot "phase6_latest_launch.json"
$scriptPath = Join-Path $RepoRoot "scripts\tradex_meemee_canonical_export_runner_phase5.py"
$arguments = @(
    $scriptPath,
    "--phase3-dir", $Phase3Dir,
    "--export-root", $ExportRoot,
    "--db-path", $DbPath,
    "--batch-size", "$BatchSize",
    "--max-batches", "$MaxBatches",
    "--timeout-seconds", "$TimeoutSeconds"
)
$process = Start-Process `
    -FilePath $PythonExe `
    -ArgumentList $arguments `
    -WorkingDirectory $RepoRoot `
    -WindowStyle Hidden `
    -RedirectStandardOutput $stdoutPath `
    -RedirectStandardError $stderrPath `
    -PassThru
$payload = [ordered]@{
    schema_version = "tradex_meemee_canonical_export_phase6_launch_v1"
    launched_at = (Get-Date).ToUniversalTime().ToString("o")
    pid = $process.Id
    repo_root = $RepoRoot
    phase3_dir = $Phase3Dir
    export_root = $ExportRoot
    db_path = $DbPath
    batch_size = $BatchSize
    max_batches = $MaxBatches
    timeout_seconds = $TimeoutSeconds
    stdout_path = $stdoutPath
    stderr_path = $stderrPath
    monitor_progress_glob = (Join-Path $ExportRoot "runs\*\phase5_run_progress.json")
    monitor_latest_audit = (Join-Path $ExportRoot "phase5_latest_run_audit.json")
    non_scope = @("model training", "production ranking mutation", "runtime DB write", "MeeMee UI mutation")
}
$json = $payload | ConvertTo-Json -Depth 4
Set-Content -LiteralPath $launchPath -Value $json -Encoding UTF8
Set-Content -LiteralPath $latestPath -Value $json -Encoding UTF8
Write-Output $launchPath
