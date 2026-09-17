$ErrorActionPreference = "Continue"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $root
$output = @()
$outputText = ''
foreach ($attempt in 1..8) {
  $output = docker compose exec -T flink-jobmanager /opt/flink/bin/sql-client.sh `
    -f "/opt/flink/usrlib/sql/00_bootstrap.sql" 2>&1
  $output
  $outputText = $output | Out-String
  if ($LASTEXITCODE -eq 0 -and $outputText -notmatch "\[ERROR\]") { break }
  $coordinatorStarting = $outputText -match 'CoordinatorEventProcessor is not initialized yet|Alive tablet server is empty'
  if (-not $coordinatorStarting -or $attempt -eq 8) { break }
  Write-Warning "Fluss coordinator is still initializing (attempt $attempt/8); retrying in 5 seconds."
  Start-Sleep -Seconds 5
}
$knownNonEmptyPaimonConflict = $outputText -match "already exists in Paimon catalog, and the table is not empty"
if ($LASTEXITCODE -ne 0 -or $outputText -match "\[ERROR\]") {
  if ($knownNonEmptyPaimonConflict) {
    throw "Existing Fluss/Paimon metadata cannot be registered safely. Re-run start-stack.ps1 with -RebuildRuntimeSchema after confirming this demo data may be rebuilt."
  }
  throw "Fluss/Paimon table bootstrap failed"
}
docker compose exec -T -u 0 flink-jobmanager chown -R flink:flink /warehouse
if ($LASTEXITCODE -ne 0) { throw "Warehouse ownership normalization failed" }
Write-Host "Fluss hot tables and native Paimon offline tables are ready."
