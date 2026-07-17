$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $root

$overview = Invoke-RestMethod -Uri "http://127.0.0.1:8082/jobs/overview" -TimeoutSec 10
$running = @($overview.jobs | Where-Object {
  $_.name -eq "mysql-cdc-to-paimon" -and $_.state -eq "RUNNING"
})
if ($running.Count -gt 0) {
  Write-Host "Flink CDC pipeline is already running: $($running[0].jid)"
  exit 0
}

$log = "/tmp/mysql-cdc-to-paimon.log"
$command = "nohup /opt/flink-cdc/bin/flink-cdc.sh /opt/flink-cdc/pipelines/mysql-to-paimon.yaml --flink-home /opt/flink -t remote -Drest.address=flink-jobmanager -Drest.port=8081 > $log 2>&1 &"
docker compose exec -d flink-jobmanager /bin/bash -lc $command
if ($LASTEXITCODE -ne 0) { throw "Failed to submit the Flink CDC pipeline." }

$deadline = (Get-Date).AddMinutes(2)
do {
  Start-Sleep -Seconds 3
  $overview = Invoke-RestMethod -Uri "http://127.0.0.1:8082/jobs/overview" -TimeoutSec 10
  $job = @($overview.jobs | Where-Object {
    $_.name -eq "mysql-cdc-to-paimon" -and $_.state -in @("CREATED", "RUNNING", "RESTARTING", "FAILED")
  } | Sort-Object 'start-time' -Descending) | Select-Object -First 1
  if ($job -and $job.state -eq "RUNNING") {
    Write-Host "Flink CDC pipeline is running: $($job.jid)"
    exit 0
  }
  if ($job -and $job.state -in @("FAILED", "CANCELED")) {
    docker compose exec -T flink-jobmanager /bin/bash -lc "tail -n 120 $log"
    throw "Flink CDC pipeline entered state $($job.state)."
  }
} while ((Get-Date) -lt $deadline)

docker compose exec -T flink-jobmanager /bin/bash -lc "tail -n 120 $log"
throw "Flink CDC pipeline did not reach RUNNING within two minutes."
