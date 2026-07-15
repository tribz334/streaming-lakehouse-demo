$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $false
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $root

foreach ($sqlFile in @("00_catalogs_and_tables.sql", "09_thesis_model_tables.sql")) {
  $output = docker compose --profile core exec -T flink-jobmanager `
    /opt/flink/bin/sql-client.sh -f "/opt/flink/usrlib/sql/$sqlFile" 2>&1
  $output
  if ($LASTEXITCODE -ne 0 -or $output -match "\[ERROR\]") {
    throw "Flink DDL initialization failed: $sqlFile"
  }
}

Write-Host "Flink sources, demo tables, and thesis Appendix A warehouse tables created."
