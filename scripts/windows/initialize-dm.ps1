param([Parameter(Mandatory=$true)][string]$BizDate)
$ErrorActionPreference = 'Continue'
if ($PSVersionTable.PSVersion.Major -ge 7) {
  $PSNativeCommandUseErrorActionPreference = $false
}
$root = Resolve-Path (Join-Path $PSScriptRoot '..\..')
Set-Location $root
$day = [DateTime]::ParseExact($BizDate,'yyyy-MM-dd',[Globalization.CultureInfo]::InvariantCulture)
$source = Get-Content -Raw (Join-Path $root 'flink\sql\11_initialize_dm.sql')
$rendered = $source.Replace('__BIZ_DATE__',$BizDate).Replace('__DATE_MINUS_6__',$day.AddDays(-6).ToString('yyyy-MM-dd')).Replace('__DATE_MINUS_29__',$day.AddDays(-29).ToString('yyyy-MM-dd'))
$tempFile = Join-Path ([IO.Path]::GetTempPath()) "paimon-dm-init-$BizDate-$PID.sql"
try {
  [IO.File]::WriteAllText($tempFile,$rendered,(New-Object Text.UTF8Encoding($false)))
  docker compose cp $tempFile flink-jobmanager:/tmp/initialize-dm.sql
  if ($LASTEXITCODE -ne 0) { throw 'Failed to copy DM initialization SQL' }
  $output = docker compose exec -T flink-jobmanager /opt/flink/bin/sql-client.sh -f /tmp/initialize-dm.sql 2>&1
  $output
  if ($LASTEXITCODE -ne 0 -or ($output | Out-String) -match '\[ERROR\]') {
    throw 'Paimon DM initialization failed'
  }
} finally {
  Remove-Item -LiteralPath $tempFile -Force -ErrorAction SilentlyContinue
}
Write-Host "DM full snapshot base partition $BizDate initialized."
