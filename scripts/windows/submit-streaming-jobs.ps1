$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $root

$projectPath = (Resolve-Path (Join-Path $root "flink-java")).Path
docker run --rm -v maven-cache:/root/.m2 -v "${projectPath}:/workspace" `
  -w /workspace maven:3.9.11-eclipse-temurin-17 mvn -q -DskipTests package
if ($LASTEXITCODE -ne 0) { throw "Java realtime job build failed" }

$running = docker compose exec -T flink-jobmanager flink list -r 2>&1 | Out-String
if ($running -match "starrocks_realtime_attribution_metric_sink") {
  Write-Host "Realtime Java attribution job is already running; no duplicate job was submitted."
  exit 0
}

docker compose exec -T flink-jobmanager flink run -d `
  -c cn.edu.ustc.lakehouse.realtime.DwsAdMetric `
  /opt/flink/usrlib/java/ad-realtime-metric-job.jar `
  --startup-mode latest `
  --order-mysql-hostname mysql `
  --order-mysql-database ad_ods `
  --order-mysql-table ad_order `
  --order-mysql-server-id 5501-5508 `
  --order-mysql-startup-mode latest-offset `
  --parallelism 1
if ($LASTEXITCODE -ne 0) { throw "Java realtime job submission failed" }

Write-Host "Kafka ad_log + MySQL CDC order Last Click job submitted. Check http://127.0.0.1:8082."
