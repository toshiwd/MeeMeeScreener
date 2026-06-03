param(
    [string]$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path,
    [string]$PythonExe = "python",
    [string]$Phase7Dir = "G:\Tradex\meemee_training_preparation_phase7\20260602T080906Z-meemee_training_preparation_phase7",
    [string]$OutputRoot = "G:\Tradex\meemee_image_linear_baseline_phase8",
    [int]$BatchSize = 512
)

$ErrorActionPreference = "Stop"
$opsRoot = Join-Path $OutputRoot "ops"
New-Item -ItemType Directory -Path $opsRoot -Force | Out-Null
$tag = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssfffZ")
$stdoutPath = Join-Path $opsRoot "$tag-phase8.stdout.log"
$stderrPath = Join-Path $opsRoot "$tag-phase8.stderr.log"
$launchPath = Join-Path $opsRoot "$tag-phase8.launch.json"
$latestPath = Join-Path $opsRoot "phase8_latest_launch.json"
$scriptPath = Join-Path $RepoRoot "scripts\tradex_meemee_image_linear_baseline_phase8.py"
$arguments = @(
    $scriptPath,
    "--phase7-dir", $Phase7Dir,
    "--output-root", $OutputRoot,
    "--batch-size", "$BatchSize"
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
    schema_version = "tradex_meemee_image_linear_baseline_phase8_launch_v1"
    launched_at = (Get-Date).ToUniversalTime().ToString("o")
    pid = $process.Id
    repo_root = $RepoRoot
    phase7_dir = $Phase7Dir
    output_root = $OutputRoot
    batch_size = $BatchSize
    stdout_path = $stdoutPath
    stderr_path = $stderrPath
    monitor_progress_glob = (Join-Path $OutputRoot "*\phase8_progress.json")
    monitor_latest_audit = (Join-Path $OutputRoot "phase8_latest_audit.json")
    non_scope = @("CNN training", "fusion", "probability calibration", "production ranking mutation", "runtime DB write", "MeeMee UI mutation")
}
$json = $payload | ConvertTo-Json -Depth 4
Set-Content -LiteralPath $launchPath -Value $json -Encoding UTF8
Set-Content -LiteralPath $latestPath -Value $json -Encoding UTF8
Write-Output $launchPath
