$ErrorActionPreference = "Continue"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $root
$output = docker compose exec -T flink-jobmanager /opt/flink/bin/sql-client.sh `
  -f "/opt/flink/usrlib/sql/00_bootstrap.sql" 2>&1
$output
if ($LASTEXITCODE -ne 0 -or ($output | Out-String) -match "\[ERROR\]") { throw "Fluss/Paimon table bootstrap failed" }
docker compose exec -T -u 0 flink-jobmanager chown -R flink:flink /warehouse
if ($LASTEXITCODE -ne 0) { throw "Warehouse ownership normalization failed" }
Write-Host "Fluss hot tables and native Paimon offline tables are ready."
