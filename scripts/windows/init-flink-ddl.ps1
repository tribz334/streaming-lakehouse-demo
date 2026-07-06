$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $root

docker compose --profile core exec -T flink-jobmanager `
  /opt/flink/bin/sql-client.sh -f /opt/flink/usrlib/sql/00_catalogs_and_tables.sql

Write-Host "Flink source tables and Paimon warehouse tables created."
