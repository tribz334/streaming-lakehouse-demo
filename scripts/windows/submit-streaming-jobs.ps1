$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $root

$projectPath = (Resolve-Path (Join-Path $root "flink-java")).Path
docker run --rm -v maven-cache:/root/.m2 -v "${projectPath}:/workspace" `
  -w /workspace maven:3.9.11-eclipse-temurin-17 mvn -q -DskipTests package
if ($LASTEXITCODE -ne 0) { throw "Java realtime job build failed" }

$jobJar = Join-Path $projectPath "target\ad-realtime-metric-job.jar"
docker cp $jobJar ustc_lakehouse-flink-jobmanager-1:/tmp/ad-realtime-metric-job.jar
if ($LASTEXITCODE -ne 0) { throw "Copying Java realtime job JAR failed" }

$running = docker compose exec -T flink-jobmanager flink list -r 2>&1 | Out-String
if ($running -match "mysql-cdc-direct-to-dim") {
  Write-Host "MySQL CDC direct-to-DIM job is already running."
} else {
  $command = "nohup /opt/flink/bin/sql-client.sh -f /opt/flink/usrlib/sql/05_mysql_cdc_direct_to_dim.sql > /tmp/mysql-cdc-direct-to-dim.log 2>&1 &"
  docker compose exec -d flink-jobmanager /bin/bash -lc $command
  if ($LASTEXITCODE -ne 0) { throw "MySQL CDC direct-to-DIM submission failed" }
}

if ($running -match "starrocks_realtime_attribution_metric_sink") {
  Write-Host "Realtime Java attribution job is already running; no duplicate job was submitted."
} else {
  docker compose exec -T flink-jobmanager flink run -d `
    -c cn.edu.ustc.lakehouse.realtime.job.DwsAdMetric `
    /tmp/ad-realtime-metric-job.jar `
    --startup-mode latest `
    --order-mysql-hostname mysql `
    --order-mysql-database ad_ods `
    --order-mysql-table order_detail `
    --order-mysql-server-id 5501-5508 `
    --order-mysql-startup-mode latest-offset `
    --parallelism 1
  if ($LASTEXITCODE -ne 0) { throw "Java realtime job submission failed" }
}

if ($running -match "dwd-order-lifecycle-acc") {
  Write-Host "Paimon accumulating order lifecycle job is already running."
} else {
  $command = "nohup /opt/flink/bin/sql-client.sh -f /opt/flink/usrlib/sql/02_dwd_order_lifecycle.sql > /tmp/dwd-order-lifecycle-acc.log 2>&1 &"
  docker compose exec -d flink-jobmanager /bin/bash -lc $command
  if ($LASTEXITCODE -ne 0) { throw "Order lifecycle SQL job submission failed" }
}

if ($running -match "ods-log-and-dwd-ad-facts") {
  Write-Host "Paimon ODS log and DWD advertising fact job is already running."
} else {
  $command = "nohup /opt/flink/bin/sql-client.sh -f /opt/flink/usrlib/sql/04_dwd_ad_facts_to_paimon.sql > /tmp/dwd-ad-facts-to-paimon.log 2>&1 &"
  docker compose exec -d flink-jobmanager /bin/bash -lc $command
  if ($LASTEXITCODE -ne 0) { throw "DWD advertising fact SQL job submission failed" }
}

Write-Host "Realtime attribution and all Paimon DWD jobs submitted. Check http://127.0.0.1:18082."
