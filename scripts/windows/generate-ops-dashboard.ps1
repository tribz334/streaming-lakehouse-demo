$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $root

$dashboardDir = Join-Path $root "ops-dashboard"
New-Item -ItemType Directory -Force -Path $dashboardDir | Out-Null

function Try-GetJson {
  param([Parameter(Mandatory = $true)][string]$Uri)
  try {
    return Invoke-RestMethod -Uri $Uri -TimeoutSec 5
  } catch {
    return $null
  }
}

function Escape-Html {
  param([AllowNull()][string]$Value)
  if ($null -eq $Value) { return "" }
  return [System.Net.WebUtility]::HtmlEncode($Value)
}

function Read-StarRocksRows {
  $sql = @"
USE ad_ads;
SELECT 'metrics' AS view_name, COUNT(*) AS rows_count FROM v_realtime_ad_metrics
UNION ALL SELECT 'retention', COUNT(*) FROM v_advertiser_retention
UNION ALL SELECT 'attribution', COUNT(*) FROM v_attribution_summary
UNION ALL SELECT 'fraud', COUNT(*) FROM v_fraud_signal_summary;
"@
  $tempFile = Join-Path ([System.IO.Path]::GetTempPath()) ("ops-starrocks-{0}.sql" -f ([guid]::NewGuid()))
  try {
    Set-Content -Path $tempFile -Value $sql -Encoding UTF8
    docker cp $tempFile ustc_lakehouse-starrocks-1:/tmp/ops_starrocks.sql | Out-Null
    $output = @(docker compose --profile olap exec -T starrocks bash -lc "mysql -h127.0.0.1 -P9030 -uroot < /tmp/ops_starrocks.sql" 2>$null)
    $rows = @()
    foreach ($line in $output | Select-Object -Skip 1) {
      $parts = $line -split "\s+"
      if ($parts.Count -ge 2) {
        $rows += [pscustomobject]@{ name = $parts[0]; count = $parts[1] }
      }
    }
    return $rows
  } catch {
    return @()
  } finally {
    Remove-Item -Path $tempFile -ErrorAction SilentlyContinue
  }
}

$flinkJobs = Try-GetJson "http://127.0.0.1:8082/jobs"
$prometheusReady = $false
try {
  Invoke-RestMethod -Uri "http://127.0.0.1:19090/-/ready" -TimeoutSec 5 | Out-Null
  $prometheusReady = $true
} catch {}

$prometheusTargets = Try-GetJson "http://127.0.0.1:19090/api/v1/targets"
$starrocksRows = Read-StarRocksRows
$grafanaHealth = Try-GetJson "http://127.0.0.1:13000/api/health"
$lokiReady = $false
try {
  Invoke-RestMethod -Uri "http://127.0.0.1:13100/ready" -TimeoutSec 3 | Out-Null
  $lokiReady = $true
} catch {}
$dolphinReady = $false
try {
  Invoke-RestMethod -Uri "http://127.0.0.1:12345/dolphinscheduler" -TimeoutSec 3 | Out-Null
  $dolphinReady = $true
} catch {}

$metadataPath = Join-Path $root "datahub/metadata/lakehouse_metadata.json"
$metadata = $null
if (Test-Path $metadataPath) {
  $metadata = Get-Content -Raw -Path $metadataPath | ConvertFrom-Json
}

$schedulerRunPath = Join-Path $root "dolphinscheduler/runs/latest-run.json"
$schedulerRun = $null
if (Test-Path $schedulerRunPath) {
  $schedulerRun = Get-Content -Raw -Path $schedulerRunPath | ConvertFrom-Json
}

$runningJobs = 0
$finishedJobs = 0
$canceledJobs = 0
if ($flinkJobs -and $flinkJobs.jobs) {
  $runningJobs = @($flinkJobs.jobs | Where-Object { $_.status -eq "RUNNING" }).Count
  $finishedJobs = @($flinkJobs.jobs | Where-Object { $_.status -eq "FINISHED" }).Count
  $canceledJobs = @($flinkJobs.jobs | Where-Object { $_.status -eq "CANCELED" }).Count
}

$targetRows = ""
if ($prometheusTargets -and $prometheusTargets.data.activeTargets) {
  foreach ($target in $prometheusTargets.data.activeTargets) {
    $targetRows += "<tr><td>$(Escape-Html $target.labels.job)</td><td>$(Escape-Html $target.scrapeUrl)</td><td>$(Escape-Html $target.health)</td></tr>`n"
  }
} else {
  $targetRows = "<tr><td colspan=""3"">Prometheus target API unavailable.</td></tr>"
}

$viewRows = ""
foreach ($row in $starrocksRows) {
  $viewRows += "<tr><td>$(Escape-Html $row.name)</td><td>$(Escape-Html $row.count)</td></tr>`n"
}
if ($viewRows -eq "") {
  $viewRows = "<tr><td colspan=""2"">StarRocks view counts unavailable.</td></tr>"
}

$lineageRows = ""
if ($metadata -and $metadata.lineage) {
  foreach ($edge in $metadata.lineage) {
    $lineageRows += "<tr><td>$(Escape-Html $edge.job)</td><td>$(Escape-Html $edge.upstream)</td><td>$(Escape-Html $edge.downstream)</td></tr>`n"
  }
} else {
  $lineageRows = "<tr><td colspan=""3"">Governance metadata not exported yet.</td></tr>"
}

$generatedAt = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
$prometheusStatus = if ($prometheusReady) { "READY" } else { "DOWN" }
$metadataCount = if ($metadata -and $metadata.datasets) { @($metadata.datasets).Count } else { 0 }
$grafanaStatus = if ($grafanaHealth) { "READY" } else { "FALLBACK" }
$lokiStatus = if ($lokiReady) { "READY" } else { "FALLBACK" }
$dolphinStatus = if ($dolphinReady) { "READY" } else { "LOCAL" }
$schedulerStatus = if ($schedulerRun) { $schedulerRun.status } else { "NO_RUN" }
$schedulerDuration = if ($schedulerRun) { "$($schedulerRun.duration_seconds)s" } else { "-" }

$html = @"
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>USTC Lakehouse Ops Dashboard</title>
  <style>
    body { margin: 0; font-family: Arial, "Microsoft YaHei", sans-serif; background: #f6f7f9; color: #1d252d; }
    header { padding: 24px 32px; background: #15202b; color: white; }
    h1 { margin: 0 0 6px; font-size: 26px; letter-spacing: 0; }
    main { padding: 24px 32px 40px; }
    .grid { display: grid; grid-template-columns: repeat(4, minmax(160px, 1fr)); gap: 14px; margin-bottom: 22px; }
    .card { background: white; border: 1px solid #dde3ea; border-radius: 8px; padding: 16px; }
    .label { font-size: 13px; color: #65717d; margin-bottom: 8px; }
    .value { font-size: 28px; font-weight: 700; }
    section { background: white; border: 1px solid #dde3ea; border-radius: 8px; padding: 18px; margin: 16px 0; }
    h2 { font-size: 18px; margin: 0 0 14px; }
    table { width: 100%; border-collapse: collapse; font-size: 13px; }
    th, td { padding: 9px 8px; border-bottom: 1px solid #e8edf2; text-align: left; vertical-align: top; }
    th { color: #53616f; font-weight: 600; background: #f9fafb; }
    .ok { color: #147d4f; }
    .warn { color: #9a5b00; }
    @media (max-width: 900px) { .grid { grid-template-columns: repeat(2, minmax(140px, 1fr)); } main, header { padding-left: 18px; padding-right: 18px; } }
  </style>
</head>
<body>
  <header>
    <h1>USTC Streaming Lakehouse Ops Dashboard</h1>
    <div>Generated at $generatedAt from local Flink, Prometheus, StarRocks, and governance metadata.</div>
  </header>
  <main>
    <div class="grid">
      <div class="card"><div class="label">Flink Running Jobs</div><div class="value">$runningJobs</div></div>
      <div class="card"><div class="label">Flink Finished Jobs</div><div class="value">$finishedJobs</div></div>
      <div class="card"><div class="label">Prometheus</div><div class="value $(if ($prometheusReady) { "ok" } else { "warn" })">$prometheusStatus</div></div>
      <div class="card"><div class="label">Governance Datasets</div><div class="value">$metadataCount</div></div>
      <div class="card"><div class="label">Grafana</div><div class="value $(if ($grafanaHealth) { "ok" } else { "warn" })">$grafanaStatus</div></div>
      <div class="card"><div class="label">Loki</div><div class="value $(if ($lokiReady) { "ok" } else { "warn" })">$lokiStatus</div></div>
      <div class="card"><div class="label">Scheduler</div><div class="value $(if ($schedulerStatus -eq "SUCCESS") { "ok" } else { "warn" })">$schedulerStatus</div></div>
      <div class="card"><div class="label">Workflow Duration</div><div class="value">$schedulerDuration</div></div>
    </div>
    <section>
      <h2>Runtime Fallbacks</h2>
      <table><thead><tr><th>Component</th><th>Status</th><th>Fallback Artifact</th></tr></thead><tbody>
        <tr><td>Grafana</td><td>$grafanaStatus</td><td>ops-dashboard/index.html</td></tr>
        <tr><td>Loki</td><td>$lokiStatus</td><td>Docker container logs plus Prometheus targets</td></tr>
        <tr><td>DolphinScheduler</td><td>$dolphinStatus</td><td>dolphinscheduler/dashboard/index.html</td></tr>
      </tbody></table>
    </section>
    <section>
      <h2>StarRocks BI Views</h2>
      <table><thead><tr><th>View</th><th>Rows</th></tr></thead><tbody>$viewRows</tbody></table>
    </section>
    <section>
      <h2>Prometheus Targets</h2>
      <table><thead><tr><th>Job</th><th>Scrape URL</th><th>Health</th></tr></thead><tbody>$targetRows</tbody></table>
    </section>
    <section>
      <h2>Offline Governance Lineage</h2>
      <table><thead><tr><th>Job</th><th>Upstream</th><th>Downstream</th></tr></thead><tbody>$lineageRows</tbody></table>
    </section>
  </main>
</body>
</html>
"@

$dashboardPath = Join-Path $dashboardDir "index.html"
Set-Content -Path $dashboardPath -Value $html -Encoding UTF8
Write-Host "Generated ops dashboard at $dashboardPath"
