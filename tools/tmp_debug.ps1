$dir = 'C:\work\meemee-screener\data'
$file = Get-ChildItem -LiteralPath $dir -Filter *.ebk | Select-Object -First 1
$lines = Get-Content -LiteralPath $file.FullName -Encoding UTF8
$lines.GetType().FullName | Write-Output
