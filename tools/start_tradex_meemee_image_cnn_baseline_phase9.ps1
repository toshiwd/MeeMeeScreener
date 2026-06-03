param(
    [string]$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path,
    [string]$PythonExe = "G:\Tradex\envs\image-cnn-phase2b\Scripts\python.exe",
    [string]$Phase7Dir = "G:\Tradex\meemee_training_preparation_phase7\20260602T080906Z-meemee_training_preparation_phase7",
    [string]$Phase8Dir = "G:\Tradex\meemee_image_linear_baseline_phase8\20260602T081223Z-meemee_image_linear_baseline_phase8",
    [string]$OutputRoot = "G:\Tradex\meemee_image_cnn_baseline_phase9",
    [int]$Epochs = 4,
    [int]$BatchSize = 128
)

$ErrorActionPreference = "Stop"
$opsRoot = Join-Path $OutputRoot "ops"
New-Item -ItemType Directory -Path $opsRoot -Force | Out-Null
$tag = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssfffZ")
$stdoutPath = Join-Path $opsRoot "$tag-phase9.stdout.log"
$stderrPath = Join-Path $opsRoot "$tag-phase9.stderr.log"
$launchPath = Join-Path $opsRoot "$tag-phase9.launch.json"
$latestPath = Join-Path $opsRoot "phase9_latest_launch.json"
$scriptPath = Join-Path $RepoRoot "scripts\tradex_meemee_image_cnn_baseline_phase9.py"
$arguments = @($scriptPath, "--phase7-dir", $Phase7Dir, "--phase8-dir", $Phase8Dir, "--output-root", $OutputRoot, "--epochs", "$Epochs", "--batch-size", "$BatchSize")
$process = Start-Process -FilePath $PythonExe -ArgumentList $arguments -WorkingDirectory $RepoRoot -WindowStyle Hidden -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath -PassThru
$payload = [ordered]@{
    schema_version = "tradex_meemee_image_cnn_baseline_phase9_launch_v1"
    launched_at = (Get-Date).ToUniversalTime().ToString("o")
    pid = $process.Id
    python_exe = $PythonExe
    phase7_dir = $Phase7Dir
    phase8_dir = $Phase8Dir
    output_root = $OutputRoot
    epochs = $Epochs
    batch_size = $BatchSize
    stdout_path = $stdoutPath
    stderr_path = $stderrPath
    monitor_progress_glob = (Join-Path $OutputRoot "*\phase9_progress.json")
    monitor_latest_audit = (Join-Path $OutputRoot "phase9_latest_audit.json")
    non_scope = @("fusion", "probability calibration", "production ranking mutation", "runtime DB write", "MeeMee UI mutation")
}
$json = $payload | ConvertTo-Json -Depth 4
Set-Content -LiteralPath $launchPath -Value $json -Encoding UTF8
Set-Content -LiteralPath $latestPath -Value $json -Encoding UTF8
Write-Output $launchPath
