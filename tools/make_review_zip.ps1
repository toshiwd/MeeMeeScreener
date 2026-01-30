param(
  [string]$Root = "",
  [string]$ZipPath = ""
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
$zipOut = if ($ZipPath) { $ZipPath } else { Join-Path $rootPath 'review-src.zip' }
$reviewDir = Join-Path $rootPath 'review_pack'
New-Item -ItemType Directory -Force -Path $reviewDir | Out-Null
$logPath = Join-Path $reviewDir 'review_zip_check.txt'

if (Test-Path $zipOut) { Remove-Item -Force $zipOut }

& git -C $rootPath archive -o $zipOut HEAD
if (-not (Test-Path $zipOut)) {
  Set-Content -Path $logPath -Value 'zip creation failed'
  exit 1
}

$py = @"
import re, zipfile, sys
zip_path = sys.argv[1]
z = zipfile.ZipFile(zip_path)
bad = [n for n in z.namelist() if re.search(r'(?i)(node_modules|\\\\.venv|dist|build|out|release)/', n)
       or re.search(r'(?i)\\\\.(exe|dll|so|dylib|zip|7z)$', n)
       or re.search(r'(?i)^\\\\.env', n)
       or re.search(r'(?i)review_pack/', n)
       or re.search(r'(?i)review-src\\\\.zip', n)]
print('bad_entries', len(bad))
print('sample', bad[:10])
"@

$errPath = Join-Path $reviewDir 'review_zip_check.err.txt'
if (Test-Path $errPath) { Remove-Item -Force $errPath }
$pyPath = Join-Path $reviewDir 'review_zip_check.py'
Set-Content -Path $pyPath -Value $py
$proc = Start-Process -FilePath 'python' -ArgumentList @($pyPath, $zipOut) -WorkingDirectory $rootPath -NoNewWindow -Wait -PassThru -RedirectStandardOutput $logPath -RedirectStandardError $errPath
if (Test-Path $errPath) {
  $errContent = Get-Content -Path $errPath -Raw
  if ($errContent) {
    Add-Content -Path $logPath -Value ''
    Add-Content -Path $logPath -Value '--- stderr ---'
    Add-Content -Path $logPath -Value $errContent
  }
}
if ($proc.ExitCode -ne 0) {
  exit 1
}

$logContent = Get-Content -Path $logPath -Raw
if ($logContent -match 'bad_entries\s+(\d+)') {
  $count = [int]$matches[1]
  if ($count -gt 0) {
    exit 1
  }
}

exit 0
