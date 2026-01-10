param(
    [Parameter(Mandatory = $true)]
    [string]$InputFile,
    [Parameter(Mandatory = $true)]
    [string]$OutputFile
)

$ErrorActionPreference = "Stop"

function Resolve-InputFile {
    param([string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) {
        throw "Input file not found: $Path"
    }
    $item = Get-Item -LiteralPath $Path
    if ($item.PSIsContainer) {
        $candidate = Get-ChildItem -LiteralPath $Path -Filter *.ebk -File |
            Sort-Object LastWriteTime -Descending |
            Select-Object -First 1
        if (-not $candidate) {
            throw "No .ebk file found in: $Path"
        }
        return $candidate.FullName
    }
    return $item.FullName
}

function Normalize-Line {
    param([string]$Line)
    $normalized = $Line -replace "`0", ""
    $normalized = $normalized -replace [string][char]0xFEFF, ""
    $normalized = $normalized -replace [string][char]0xFF03, "#"
    return $normalized.Trim()
}

$resolvedInput = Resolve-InputFile -Path $InputFile
$lines = Get-Content -LiteralPath $resolvedInput -Encoding UTF8 -ErrorAction Stop
$codes = New-Object System.Collections.Generic.List[string]

foreach ($line in $lines) {
    $normalized = Normalize-Line -Line $line
    if ([string]::IsNullOrWhiteSpace($normalized)) { continue }
    if ($normalized.StartsWith("#")) { continue }

    $match = [regex]::Match($normalized, "(?i)JP#(?<code>\\d{4,5})")
    if ($match.Success) {
        $codes.Add($match.Groups["code"].Value)
        continue
    }
    $match = [regex]::Match($normalized, "(?<code>\\d{4,5})")
    if ($match.Success) {
        $codes.Add($match.Groups["code"].Value)
        continue
    }
}

if ($codes.Count -eq 0) {
    $raw = Get-Content -LiteralPath $resolvedInput -Raw -Encoding UTF8 -ErrorAction Stop
    $raw = Normalize-Line -Line $raw
    $matches = [regex]::Matches($raw, "(?i)JP#(?<code>\\d{4,5})")
    foreach ($match in $matches) {
        $codes.Add($match.Groups["code"].Value)
    }
    if ($codes.Count -eq 0) {
        $matches = [regex]::Matches($raw, "(?<code>\\d{4,5})")
        foreach ($match in $matches) {
            $codes.Add($match.Groups["code"].Value)
        }
    }
}

$deduped = $codes | Where-Object { $_ } | Sort-Object {[int]$_} -Unique
if ($deduped.Count -eq 0) {
    throw "No codes found in $InputFile"
}

$outDir = Split-Path -Parent $OutputFile
if ($outDir -and -not (Test-Path $outDir)) {
    New-Item -ItemType Directory -Path $outDir -Force | Out-Null
}

Set-Content -Path $OutputFile -Value $deduped -Encoding ASCII
Write-Host "Wrote $(($deduped | Measure-Object).Count) codes to $OutputFile"
