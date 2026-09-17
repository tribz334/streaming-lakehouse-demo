param([string]$BizDate = (Get-Date).AddDays(-1).ToString('yyyy-MM-dd'))
$ErrorActionPreference = 'Continue'
if ($PSVersionTable.PSVersion.Major -ge 7) {
  $PSNativeCommandUseErrorActionPreference = $false
}
$root = Resolve-Path (Join-Path $PSScriptRoot '..\..')
Set-Location $root
$day = [DateTime]::ParseExact($BizDate,'yyyy-MM-dd',[Globalization.CultureInfo]::InvariantCulture)
$dwsSource = Get-Content -Raw (Join-Path $root 'flink\sql\04_daily_dws.sql')
$adsDmSource = Get-Content -Raw (Join-Path $root 'flink\sql\10_daily_offline.sql')
$source = $dwsSource + [Environment]::NewLine + $adsDmSource
$rendered = $source.Replace('__BIZ_DATE_COMPACT__',$day.ToString('yyyyMMdd')).Replace('__BIZ_DATE__',$BizDate).Replace('__PREV_DATE__',$day.AddDays(-1).ToString('yyyy-MM-dd')).Replace('__DATE_MINUS_7__',$day.AddDays(-7).ToString('yyyy-MM-dd')).Replace('__DATE_MINUS_30__',$day.AddDays(-30).ToString('yyyy-MM-dd'))
$tempFile = Join-Path ([IO.Path]::GetTempPath()) "paimon-offline-$BizDate-$PID.sql"
try {
  [IO.File]::WriteAllText($tempFile,$rendered,(New-Object Text.UTF8Encoding($false)))
  docker compose cp $tempFile flink-jobmanager:/tmp/run-daily-offline.sql
  if ($LASTEXITCODE -ne 0) { throw 'Failed to copy daily SQL' }
  $output = docker compose exec -T flink-jobmanager /opt/flink/bin/sql-client.sh -f /tmp/run-daily-offline.sql 2>&1
  $output
  if ($LASTEXITCODE -ne 0 -or ($output | Out-String) -match '\[ERROR\]') {
    throw 'Paimon offline batch failed'
  }

} finally {
  Remove-Item -LiteralPath $tempFile -Force -ErrorAction SilentlyContinue
}
Write-Host "Offline partition $BizDate is available through the StarRocks Paimon catalog."
