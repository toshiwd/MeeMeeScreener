$ErrorActionPreference = "Stop"

param(
  [string]$ExtractDir = $PSScriptRoot,
  [string]$WebView2InstallerUrl = "https://go.microsoft.com/fwlink/p/?LinkId=2124703",
  [string]$DotNet48InstallerUrl = "https://go.microsoft.com/fwlink/?LinkId=2085155",
  [string]$VCRedistInstallerUrl = "https://aka.ms/vs/17/release/vc_redist.x64.exe"
)

function Test-VCRedist {
  # Check for VC++ 2015-2022 Redistributable (x64)
  # Registry key for VC++ 2022 x64
  $keyPath = "HKLM:\\SOFTWARE\\WOW6432Node\\Microsoft\\VisualStudio\\14.0\\VC\\Runtimes\\x64"
  if (Test-Path $keyPath) {
    try {
        $installed = (Get-ItemProperty -Path $keyPath -Name Installed -ErrorAction Stop).Installed
        if ($installed -eq 1) { return $true }
    } catch {}
  }
  
  # Fallback check for DLL
  $dllPath = "$env:SystemRoot\System32\vcruntime140.dll"
  return (Test-Path $dllPath)
}

function Install-VCRedist {
  $installerPath = Join-Path $env:TEMP "vc_redist.x64.exe"
  Write-Host "Downloading Visual C++ Redistributable..."
  Invoke-WebRequest -Uri $VCRedistInstallerUrl -OutFile $installerPath
  Write-Host "Installing Visual C++ Redistributable..."
  Start-Process -FilePath $installerPath -ArgumentList "/install", "/quiet", "/norestart" -Wait
}

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

try {
    if (-not (Test-DotNet48)) {
        Install-DotNet48
    }

    if (-not (Test-VCRedist)) {
        Install-VCRedist
    }

    if (-not (Test-WebView2Runtime)) {
        Install-WebView2Runtime
    }

    if (-not (Test-DotNet48)) {
        Install-DotNet48
    }

    if (-not (Test-WebView2Runtime)) {
        Install-WebView2Runtime
    }

    # Ensure executables are unblocked
    try {
        Get-ChildItem -Path $ExtractDir -Recurse -Force | Unblock-File -ErrorAction SilentlyContinue
    } catch {
    }

    $exePath = Join-Path $ExtractDir "MeeMeeScreener.exe"
    if (-not (Test-Path $exePath)) {
        throw "Exe not found: $exePath"
    }

    Write-Host "Launching MeeMee Screener..."
    Start-Process -FilePath $exePath
} catch {
    Write-Host "Error occurred: $_" -ForegroundColor Red
}

Write-Host "Press Enter to exit..."
Read-Host
