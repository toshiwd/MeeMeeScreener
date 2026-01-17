$ErrorActionPreference = "Stop"

param(
  [string]$ZipPath = (Join-Path $PSScriptRoot "MeeMeeScreener-portable.zip"),
  [string]$ExtractDir = (Join-Path $PSScriptRoot "MeeMeeScreener"),
  [string]$WebView2InstallerUrl = "https://go.microsoft.com/fwlink/p/?LinkId=2124703",
  [string]$DotNet48InstallerUrl = "https://go.microsoft.com/fwlink/?LinkId=2085155"
)

function Test-WebView2Runtime {
  $paths = @(
    "${env:ProgramFiles(x86)}\Microsoft\EdgeWebView\Application\msedgewebview2.exe",
    "${env:ProgramFiles}\Microsoft\EdgeWebView\Application\msedgewebview2.exe"
  )
  foreach ($p in $paths) {
    if (Test-Path $p) { return $true }
  }
  return $false
}

function Install-WebView2Runtime {
  $installerPath = Join-Path $env:TEMP "MicrosoftEdgeWebView2RuntimeInstallerX64.exe"
  Write-Host "Downloading WebView2 Runtime installer..."
  Invoke-WebRequest -Uri $WebView2InstallerUrl -OutFile $installerPath
  Write-Host "Installing WebView2 Runtime..."
  Start-Process -FilePath $installerPath -ArgumentList "/install", "/silent", "/acceptlicenses" -Wait
}

function Test-DotNet48 {
  $keyPath = "HKLM:\\SOFTWARE\\Microsoft\\NET Framework Setup\\NDP\\v4\\Full"
  try {
    $release = (Get-ItemProperty -Path $keyPath -Name Release -ErrorAction Stop).Release
  } catch {
    return $false
  }
  return ($release -ge 528040)
}

function Install-DotNet48 {
  $installerPath = Join-Path $env:TEMP "ndp48-x86-x64-allos-enu.exe"
  Write-Host "Downloading .NET Framework 4.8 installer..."
  Invoke-WebRequest -Uri $DotNet48InstallerUrl -OutFile $installerPath
  Write-Host "Installing .NET Framework 4.8..."
  Start-Process -FilePath $installerPath -ArgumentList "/q", "/norestart" -Wait
}

if (-not (Test-DotNet48)) {
  Install-DotNet48
}

if (-not (Test-WebView2Runtime)) {
  Install-WebView2Runtime
}

if (-not (Test-Path $ZipPath)) {
  throw "Zip not found: $ZipPath"
}

try {
  Unblock-File -Path $ZipPath -ErrorAction SilentlyContinue
} catch {
}

if (-not (Test-Path $ExtractDir)) {
  Write-Host "Extracting zip to $ExtractDir..."
  Expand-Archive -Path $ZipPath -DestinationPath $ExtractDir
}

try {
  Get-ChildItem -Path $ExtractDir -Recurse -Force | Unblock-File -ErrorAction SilentlyContinue
} catch {
}

$exePath = Join-Path $ExtractDir "MeeMeeScreener.exe"
if (-not (Test-Path $exePath)) {
  throw "Exe not found: $exePath"
}

Start-Process -FilePath $exePath
