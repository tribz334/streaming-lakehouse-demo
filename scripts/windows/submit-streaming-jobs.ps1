$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $root

$jobs = @(
  "02_realtime_ods.sql",
  "03_realtime_dwd.sql",
  "04_realtime_dws_metrics.sql",
  "05_realtime_starrocks_relay.sql"
)

foreach ($job in $jobs) {
  $log = "/tmp/$job.log"
  $runFile = "/tmp/run_$job"
  $cmd = "cat /opt/flink/usrlib/sql/00_catalogs_and_tables.sql /opt/flink/usrlib/sql/$job > $runFile && nohup /opt/flink/bin/sql-client.sh -f $runFile > $log 2>&1 &"
  docker compose --profile core exec -d flink-jobmanager /bin/bash -lc $cmd
  Write-Host "Submitted $job; log inside flink-jobmanager:$log"
}

Write-Host "Streaming jobs requested. Check http://127.0.0.1:8082 for running jobs."
