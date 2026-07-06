$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $root

function Invoke-PaimonSql {
  param([Parameter(Mandatory = $true)][string]$Sql)

  $content = @"
SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';
SET 'sql-client.execution.result-mode' = 'TABLEAU';
CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = 'file:///warehouse/paimon'
);
$Sql
"@
  $tempFile = Join-Path ([System.IO.Path]::GetTempPath()) ("paimon-experiment-{0}.sql" -f ([guid]::NewGuid()))
  try {
    Set-Content -Path $tempFile -Value $content -Encoding UTF8
    docker cp $tempFile ustc_lakehouse-flink-jobmanager-1:/tmp/paimon_experiment.sql | Out-Null
    $output = @(docker compose --profile core exec -T flink-jobmanager /opt/flink/bin/sql-client.sh -f /tmp/paimon_experiment.sql 2>&1)
    if ($LASTEXITCODE -ne 0 -or $output -match "\[ERROR\]") {
      $output | ForEach-Object { Write-Host $_ }
      throw "Paimon experiment SQL failed."
    }
    return $output
  } finally {
    Remove-Item -Path $tempFile -ErrorAction SilentlyContinue
  }
}

function Get-TableauScalar {
  param([Parameter(Mandatory = $true)][AllowEmptyCollection()][AllowEmptyString()][string[]]$Output)
  foreach ($line in $Output) {
    $trimmed = $line.Trim()
    if ($trimmed -match '^\|\s*([^|]+?)\s*\|$' -and $matches[1].Trim() -notin @('scalar_value', 'EXPR$0')) {
      return $matches[1].Trim()
    }
  }
  throw "No scalar result found in Flink SQL output."
}

$runId = (Get-Date).ToString("yyyyMMddHHmmss")
$snapshotsTable = 'paimon.ad_dw.`lakehouse_feature_experiment$snapshots`'
$filesTable = 'paimon.ad_dw.`lakehouse_feature_experiment$files`'
$schemasTable = 'paimon.ad_dw.`lakehouse_feature_experiment$schemas`'
Write-Host "[1/9] Inspecting experiment schema..."
$description = Invoke-PaimonSql "DESCRIBE paimon.ad_dw.lakehouse_feature_experiment;"
$schemaEvolved = -not ($description -match "experiment_note")
if ($schemaEvolved) {
  Write-Host "[2/9] Applying schema evolution..."
  Invoke-PaimonSql "ALTER TABLE paimon.ad_dw.lakehouse_feature_experiment ADD experiment_note STRING;" | Out-Null
}

Write-Host "[3/9] Creating the historical snapshot..."
Invoke-PaimonSql @"
INSERT INTO paimon.ad_dw.lakehouse_feature_experiment
  (experiment_id, metric_value, created_at, experiment_note)
VALUES ('history_$runId', 100, CURRENT_TIMESTAMP, 'before time travel checkpoint');
"@ | Out-Null

$snapshotBefore = [long](Get-TableauScalar (Invoke-PaimonSql @"
SELECT CAST(MAX(snapshot_id) AS STRING) AS scalar_value
FROM $snapshotsTable;
"@))

Write-Host "[4/9] Creating the current snapshot..."
Invoke-PaimonSql @"
INSERT INTO paimon.ad_dw.lakehouse_feature_experiment
  (experiment_id, metric_value, created_at, experiment_note)
VALUES ('current_$runId', 200, CURRENT_TIMESTAMP, 'must not exist in previous snapshot');
"@ | Out-Null

$snapshotAfter = [long](Get-TableauScalar (Invoke-PaimonSql @"
SELECT CAST(MAX(snapshot_id) AS STRING) AS scalar_value
FROM $snapshotsTable;
"@))
Write-Host "[5/9] Verifying time travel from snapshot $snapshotBefore to $snapshotAfter..."
$historicalCount = [long](Get-TableauScalar (Invoke-PaimonSql @"
SELECT CAST(COUNT(*) AS STRING) AS scalar_value
FROM paimon.ad_dw.lakehouse_feature_experiment /*+ OPTIONS('scan.snapshot-id'='$snapshotBefore') */
WHERE experiment_id = 'current_$runId';
"@))
$latestCount = [long](Get-TableauScalar (Invoke-PaimonSql @"
SELECT CAST(COUNT(*) AS STRING) AS scalar_value
FROM paimon.ad_dw.lakehouse_feature_experiment
WHERE experiment_id = 'current_$runId';
"@))
$filesBefore = [long](Get-TableauScalar (Invoke-PaimonSql @"
SELECT CAST(COUNT(*) AS STRING) AS scalar_value
FROM $filesTable;
"@))

Write-Host "[6/9] Compacting Paimon data files..."
Invoke-PaimonSql "CALL paimon.sys.compact(``table`` => 'ad_dw.lakehouse_feature_experiment');" | Out-Null

Write-Host "[7/9] Reading post-compaction metadata..."
$filesAfter = [long](Get-TableauScalar (Invoke-PaimonSql @"
SELECT CAST(COUNT(*) AS STRING) AS scalar_value
FROM $filesTable;
"@))
$schemaCount = [long](Get-TableauScalar (Invoke-PaimonSql @"
SELECT CAST(COUNT(*) AS STRING) AS scalar_value
FROM $schemasTable;
"@))
$snapshotCount = [long](Get-TableauScalar (Invoke-PaimonSql @"
SELECT CAST(COUNT(*) AS STRING) AS scalar_value
FROM $snapshotsTable;
"@))

Write-Host "[8/9] Writing the experiment report..."
$report = [ordered]@{
  run_id = $runId
  generated_at = (Get-Date).ToString("s")
  table = "paimon.ad_dw.lakehouse_feature_experiment"
  schema_evolution_applied_this_run = $schemaEvolved
  schema_versions = $schemaCount
  snapshot_before = $snapshotBefore
  snapshot_after = $snapshotAfter
  snapshot_count = $snapshotCount
  time_travel_previous_snapshot_current_row_count = $historicalCount
  latest_snapshot_current_row_count = $latestCount
  time_travel_verified = ($historicalCount -eq 0 -and $latestCount -eq 1)
  files_before_compaction = $filesBefore
  files_after_compaction = $filesAfter
  compaction_verified = ($filesAfter -le $filesBefore)
}

$outputDir = Join-Path $root "paimon/experiments"
New-Item -ItemType Directory -Force -Path $outputDir | Out-Null
$outputPath = Join-Path $outputDir "latest-report.json"
$report | ConvertTo-Json -Depth 5 | Set-Content -Path $outputPath -Encoding UTF8
$report | Format-List
Write-Host "[9/9] Experiment completed."
Write-Host "Paimon experiment report: $outputPath"
