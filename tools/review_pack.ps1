param(
  [string]$Root = ""
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
$reviewDir = Join-Path $rootPath 'review_pack'
New-Item -ItemType Directory -Force -Path $reviewDir | Out-Null

$excludeDirs = @('.git','node_modules','.venv','dist','build','out','release','__pycache__','.pytest_cache','.next','review_pack','data_store')

function Should-SkipDir([string]$fullPath) {
  foreach ($name in $excludeDirs) {
    if ($fullPath -match ([regex]::Escape("\\$name")) ) { return $true }
  }
  if ($fullPath -match "\\data\\txt(\\|$)") { return $true }
  return $false
}

function Should-SkipPath([string]$fullPath) {
  if ($fullPath -match "\\node_modules\\") { return $true }
  if ($fullPath -match "\\dist\\") { return $true }
  if ($fullPath -match "\\build\\") { return $true }
  if ($fullPath -match "\\out\\") { return $true }
  if ($fullPath -match "\\release\\") { return $true }
  if ($fullPath -match "\\review_pack\\") { return $true }
  if ($fullPath -match "\\data_store\\") { return $true }
  return $false
}

function Write-Tree([string]$path, [int]$depth, [string]$prefix, [string]$outFile) {
  if ($depth -lt 0) { return }
  $items = Get-ChildItem -LiteralPath $path -Force -ErrorAction SilentlyContinue |
    Where-Object { -not (Should-SkipDir $_.FullName) }
  foreach ($item in $items) {
    Add-Content -Path $outFile -Value ("{0}{1}" -f $prefix, $item.Name)
    if ($item.PSIsContainer) {
      Write-Tree -path $item.FullName -depth ($depth - 1) -prefix ($prefix + '  ') -outFile $outFile
    }
  }
}

# inventory_tree.txt
$treeOut = Join-Path $reviewDir 'inventory_tree.txt'
'' | Set-Content -Path $treeOut
Write-Tree -path $rootPath -depth 4 -prefix '' -outFile $treeOut

# large_files.txt
$largeOut = Join-Path $reviewDir 'large_files.txt'
$largeFiles = Get-ChildItem -Path $rootPath -Recurse -File -ErrorAction SilentlyContinue |
  Where-Object { $_.Length -gt 100MB -and -not (Should-SkipDir $_.Directory.FullName) }
$largeFiles | Sort-Object Length -Descending | ForEach-Object {
  "{0}`t{1}" -f $_.Length, $_.FullName
} | Set-Content -Path $largeOut

# build_routes.txt
$buildOut = Join-Path $reviewDir 'build_routes.txt'
'' | Set-Content -Path $buildOut
function Add-Line([string]$text) { Add-Content -Path $buildOut -Value $text }

Add-Line '== package.json scripts =='
$pkgFiles = Get-ChildItem -Path $rootPath -Recurse -File -Filter package.json -ErrorAction SilentlyContinue |
  Where-Object { -not (Should-SkipDir $_.Directory.FullName) -and -not (Should-SkipPath $_.FullName) }
foreach ($pkg in $pkgFiles) {
  Add-Line "-- $($pkg.FullName)"
  try {
    $json = Get-Content -Raw -Path $pkg.FullName | ConvertFrom-Json
    if ($null -ne $json.scripts) {
      $json.scripts.PSObject.Properties | ForEach-Object {
        Add-Line ("  {0} = {1}" -f $_.Name, $_.Value)
      }
    } else {
      Add-Line '  (no scripts)'
    }
  } catch {
    Add-Line '  (failed to parse)'
  }
}

Add-Line ''
Add-Line '== Makefile/Taskfile/justfile =='
$taskFiles = @('Makefile','Taskfile.yml','Taskfile.yaml','justfile')
foreach ($tf in $taskFiles) {
  $files = Get-ChildItem -Path $rootPath -Recurse -File -Filter $tf -ErrorAction SilentlyContinue |
    Where-Object { -not (Should-SkipDir $_.Directory.FullName) -and -not (Should-SkipPath $_.FullName) }
  foreach ($file in $files) {
    Add-Line "-- $($file.FullName)"
    $lines = Select-String -Path $file.FullName -Pattern '(?i)build|test|lint|format' -SimpleMatch -ErrorAction SilentlyContinue
    if ($lines) {
      $lines | ForEach-Object { Add-Line ("  {0}" -f $_.Line.Trim()) }
    } else {
      Add-Line '  (no build/test/lint/format lines found)'
    }
  }
}

Add-Line ''
Add-Line '== GitHub workflows =='
$workflowRoot = Join-Path $rootPath '.github\\workflows'
if (Test-Path $workflowRoot) {
  $wfFiles = Get-ChildItem -Path $workflowRoot -Recurse -File -ErrorAction SilentlyContinue
  foreach ($wf in $wfFiles) {
    Add-Line "-- $($wf.FullName)"
    $lines = Select-String -Path $wf.FullName -Pattern '(?i)run:|uses:|build|test|lint|format|npm|pnpm|yarn|python|pytest' -ErrorAction SilentlyContinue
    if ($lines) {
      $lines | ForEach-Object { Add-Line ("  {0}" -f $_.Line.Trim()) }
    } else {
      Add-Line '  (no build/test/lint/format lines found)'
    }
  }
} else {
  Add-Line '  (CI not configured)'
}

Add-Line ''
Add-Line '== Dockerfile / compose =='
$dockerFiles = Get-ChildItem -Path $rootPath -Recurse -File -Include 'Dockerfile','docker-compose.yml','docker-compose.yaml' -ErrorAction SilentlyContinue |
  Where-Object { -not (Should-SkipDir $_.Directory.FullName) -and -not (Should-SkipPath $_.FullName) }
foreach ($df in $dockerFiles) {
  Add-Line "-- $($df.FullName)"
  $lines = Select-String -Path $df.FullName -Pattern '(?i)build|test|lint|format|RUN|CMD|ENTRYPOINT' -ErrorAction SilentlyContinue
  if ($lines) {
    $lines | ForEach-Object { Add-Line ("  {0}" -f $_.Line.Trim()) }
  } else {
    Add-Line '  (no build/test/lint/format lines found)'
  }
}

Add-Line ''
Add-Line '== extra build entrypoints (non-script) =='
$extra = @(
  'build_release.cmd',
  'tools/build_release.cmd',
  'tools/build_release.ps1',
  'run.ps1',
  'run_debug.ps1',
  'tools/portable_bootstrap.cmd',
  'tools/portable_bootstrap.ps1',
  'tools/review_pack.ps1'
) | Sort-Object -Unique
foreach ($rel in $extra) {
  $full = Join-Path $rootPath $rel
  if (Test-Path $full) { Add-Line "-- $full" }
}

Add-Line ''
Add-Line '== build duplication summary =='
Add-Line '  reason: root build_release.cmd and tools/build_release.* coexist'
Add-Line '  unify: tools/build_release.ps1 as implementation + tools/build_release.cmd as entrypoint'
Add-Line '  root build_release.cmd should be wrapper only'

# duplicates_suspects.txt
$dupOut = Join-Path $reviewDir 'duplicates_suspects.txt'
'' | Set-Content -Path $dupOut
function Add-Dup([string]$text) { Add-Content -Path $dupOut -Value $text }

Add-Dup '== lockfiles =='
$lockfiles = Get-ChildItem -Path $rootPath -Recurse -File -Include 'package-lock.json','pnpm-lock.yaml','yarn.lock','bun.lockb' -ErrorAction SilentlyContinue |
  Where-Object { -not (Should-SkipDir $_.Directory.FullName) -and -not (Should-SkipPath $_.FullName) }
$lockfiles | ForEach-Object { Add-Dup "  $($_.FullName)" }
if ($lockfiles.Count -gt 1) {
  Add-Dup "  -> Multiple lockfiles detected. Unify package manager."
}

Add-Dup ''
Add-Dup '== build config candidates =='
$buildConfigs = Get-ChildItem -Path $rootPath -Recurse -File -Include 'vite.config.*','webpack.config.*','rollup.config.*','next.config.*','tsconfig.json' -ErrorAction SilentlyContinue |
  Where-Object { -not (Should-SkipDir $_.Directory.FullName) -and -not (Should-SkipPath $_.FullName) }
$buildConfigs | ForEach-Object { Add-Dup "  $($_.FullName)" }

Add-Dup ''
Add-Dup '== README / CI command mismatch (heuristic) =='
function Get-ReadmeCommands([string]$path) {
  $cmds = @()
  $inBlock = $false
  $lines = Get-Content -Path $path -ErrorAction SilentlyContinue
  foreach ($line in $lines) {
    if ($line -match '^```') { $inBlock = -not $inBlock; continue }
    if ($line -match '^\s*(\$|PS>|>)') {
      if ($inBlock -or $line -match '^\s*(\$|PS>|>)') {
        $cmds += $line.Trim()
      }
    }
  }
  return $cmds
}

$readmeFiles = Get-ChildItem -Path $rootPath -Recurse -File -Filter 'README.md' -ErrorAction SilentlyContinue
$readmeCommands = @()
foreach ($rf in $readmeFiles) {
  $readmeCommands += Get-ReadmeCommands $rf.FullName
}
$readmeCommands = $readmeCommands | Sort-Object -Unique

$wfCommands = @()
if (Test-Path $workflowRoot) {
  $wfFiles = Get-ChildItem -Path $workflowRoot -Recurse -File -ErrorAction SilentlyContinue
  foreach ($wf in $wfFiles) {
    $content = Get-Content -Path $wf.FullName -Raw -ErrorAction SilentlyContinue
    if ($content) {
      $matches = [regex]::Matches($content, '(?i)\b(npm run \S+|pnpm \S+|yarn \S+|pytest\b|python -m \S+|make \S+)')
      foreach ($m in $matches) { $wfCommands += $m.Value }
    }
  }
  $wfCommands = $wfCommands | Sort-Object -Unique
  $readmeOnly = @($readmeCommands | Where-Object { $_ -notin $wfCommands })
  $wfOnly = @($wfCommands | Where-Object { $_ -notin $readmeCommands })
  Add-Dup "  README only: $([string]::Join(', ', $readmeOnly))"
  Add-Dup "  CI only: $([string]::Join(', ', $wfOnly))"
} else {
  Add-Dup '  CI not configured. Comparison skipped.'
}

Add-Dup ''
Add-Dup '== possibly unused scripts (not referenced by npm/pnpm/yarn run) =='
foreach ($pkg in $pkgFiles) {
  try {
    $json = Get-Content -Raw -Path $pkg.FullName | ConvertFrom-Json
    if ($null -eq $json.scripts) { continue }
    foreach ($prop in $json.scripts.PSObject.Properties) {
      $name = $prop.Name
      $pattern = "(?i)(npm run|pnpm run|yarn)\s+$name"
      $found = rg --no-messages -g '!node_modules' -g '!*dist*' -g '!*build*' -g '!*release*' -g '!*out*' $pattern $rootPath
      if (-not $found) {
        Add-Dup "  $($pkg.FullName): $name"
      }
    }
  } catch {
    continue
  }
}

Add-Dup ''
Add-Dup '== build routes duplication =='
$rootBuild = Test-Path (Join-Path $rootPath 'build_release.cmd')
$toolsBuildCmd = Test-Path (Join-Path $rootPath 'tools/build_release.cmd')
$toolsBuildPs1 = Test-Path (Join-Path $rootPath 'tools/build_release.ps1')
if ($rootBuild -or $toolsBuildCmd -or $toolsBuildPs1) {
  Add-Dup '  candidates:'
  if ($rootBuild) { Add-Dup '    - build_release.cmd (root wrapper)' }
  if ($toolsBuildCmd) { Add-Dup '    - tools/build_release.cmd (entry)' }
  if ($toolsBuildPs1) { Add-Dup '    - tools/build_release.ps1 (impl)' }
  Add-Dup '  proposal: keep tools/build_release.ps1 as implementation + tools/build_release.cmd as entry; root build_release.cmd is wrapper only.'
}

Add-Dup ''
Add-Dup '== tracked data files (risk) =='
$trackedData = git ls-files data 2>$null | Where-Object { $_ -match '\.(csv|sqlite|duckdb)$' }
if ($trackedData) {
  $trackedData | ForEach-Object { Add-Dup "  $_" }
  Add-Dup '  -> Replace with example files and move real data to data_store/.'
} else {
  Add-Dup '  (none)'
}

Add-Dup ''
Add-Dup '== tracked generated artifacts =='
$trackedArtifacts = @()
$trackedArtifacts += git ls-files review_pack 2>$null
$trackedArtifacts += git ls-files review-src.zip 2>$null
$trackedArtifacts = $trackedArtifacts | Where-Object { $_ } | Sort-Object -Unique
if ($trackedArtifacts) {
  $trackedArtifacts | ForEach-Object { Add-Dup "  $_" }
  Add-Dup '  -> Remove from git and ignore.'
} else {
  Add-Dup '  (none)'
}

Add-Dup ''
Add-Dup '== large local db warnings =='
$dbCandidates = @(
  (Join-Path $rootPath 'app/backend/stocks.duckdb'),
  (Join-Path $rootPath 'data/stocks.duckdb'),
  (Join-Path $rootPath 'data_store/stocks.duckdb')
)
$warned = $false
foreach ($dbPath in $dbCandidates) {
  if (Test-Path $dbPath) {
    $size = (Get-Item $dbPath).Length
    if ($size -gt 50MB) {
      Add-Dup ("  {0} ({1} bytes) -> too large for repo/package" -f $dbPath, $size)
      $warned = $true
    }
  }
}
if (-not $warned) {
  Add-Dup '  (none)'
}
