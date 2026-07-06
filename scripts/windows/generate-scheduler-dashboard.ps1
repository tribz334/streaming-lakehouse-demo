$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $root

$dashboardDir = Join-Path $root "dolphinscheduler/dashboard"
$runDir = Join-Path $root "dolphinscheduler/runs"
New-Item -ItemType Directory -Force -Path $dashboardDir | Out-Null
New-Item -ItemType Directory -Force -Path $runDir | Out-Null

function Escape-Html {
  param([AllowNull()][string]$Value)
  if ($null -eq $Value) { return "" }
  return [System.Net.WebUtility]::HtmlEncode($Value)
}

$latestPath = Join-Path $runDir "latest-run.json"
$latestRun = $null
if (Test-Path $latestPath) {
  $latestRun = Get-Content -Raw -Path $latestPath | ConvertFrom-Json
}

$workflowPath = Join-Path $root "dolphinscheduler/workflows/ad-lakehouse-demo.yaml"
$workflowText = if (Test-Path $workflowPath) {
  Get-Content -Raw -Path $workflowPath
} else {
  "Workflow template not found."
}

$runRows = ""
if ($latestRun -and $latestRun.steps) {
  foreach ($step in $latestRun.steps) {
    $class = if ($step.status -eq "SUCCESS") { "ok" } else { "bad" }
    $runRows += "<tr><td>$(Escape-Html $step.name)</td><td>$(Escape-Html $step.command)</td><td class=""$class"">$(Escape-Html $step.status)</td><td>$($step.duration_seconds)s</td><td>$(Escape-Html $step.error)</td></tr>`n"
  }
} else {
  $runRows = "<tr><td colspan=""5"">No workflow run has been recorded yet.</td></tr>"
}

$recentRows = ""
$runs = @(Get-ChildItem -Path $runDir -Filter "*.json" -ErrorAction SilentlyContinue |
  Where-Object { $_.Name -ne "latest-run.json" } |
  Sort-Object LastWriteTime -Descending |
  Select-Object -First 10)
foreach ($file in $runs) {
  try {
    $run = Get-Content -Raw -Path $file.FullName | ConvertFrom-Json
    $class = if ($run.status -eq "SUCCESS") { "ok" } else { "bad" }
    $recentRows += "<tr><td>$(Escape-Html $run.run_id)</td><td class=""$class"">$(Escape-Html $run.status)</td><td>$(Escape-Html $run.started_at)</td><td>$($run.duration_seconds)s</td></tr>`n"
  } catch {}
}
if ($recentRows -eq "") {
  $recentRows = "<tr><td colspan=""4"">No historical runs.</td></tr>"
}

$generatedAt = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
$latestStatus = if ($latestRun) { $latestRun.status } else { "NO_RUN" }
$latestClass = if ($latestStatus -eq "SUCCESS") { "ok" } else { "bad" }
$latestDuration = if ($latestRun) { "$($latestRun.duration_seconds)s" } else { "-" }
$latestRunId = if ($latestRun) { $latestRun.run_id } else { "-" }

$html = @"
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>USTC Lakehouse Workflow Dashboard</title>
  <style>
    body { margin: 0; font-family: Arial, "Microsoft YaHei", sans-serif; background: #f7f8fa; color: #202832; }
    header { padding: 24px 32px; background: #263238; color: white; }
    h1 { margin: 0 0 8px; font-size: 26px; letter-spacing: 0; }
    main { padding: 24px 32px 42px; }
    .summary { display: grid; grid-template-columns: repeat(3, minmax(160px, 1fr)); gap: 14px; margin-bottom: 18px; }
    .card, section { background: white; border: 1px solid #dfe5eb; border-radius: 8px; padding: 16px; }
    .label { color: #65727f; font-size: 13px; margin-bottom: 8px; }
    .value { font-size: 26px; font-weight: 700; }
    section { margin-top: 16px; }
    h2 { margin: 0 0 12px; font-size: 18px; }
    table { width: 100%; border-collapse: collapse; font-size: 13px; }
    th, td { text-align: left; padding: 9px 8px; border-bottom: 1px solid #e7edf3; vertical-align: top; }
    th { background: #f9fafb; color: #53616f; }
    pre { white-space: pre-wrap; font-size: 12px; background: #101820; color: #edf4fb; padding: 14px; border-radius: 8px; overflow: auto; }
    .ok { color: #147d4f; font-weight: 700; }
    .bad { color: #b42318; font-weight: 700; }
    @media (max-width: 900px) { .summary { grid-template-columns: 1fr; } main, header { padding-left: 18px; padding-right: 18px; } }
  </style>
</head>
<body>
  <header>
    <h1>USTC Lakehouse Workflow Dashboard</h1>
    <div>Generated at $generatedAt. This is the local runnable scheduler fallback for DolphinScheduler.</div>
  </header>
  <main>
    <div class="summary">
      <div class="card"><div class="label">Latest Run</div><div class="value">$(Escape-Html $latestRunId)</div></div>
      <div class="card"><div class="label">Status</div><div class="value $latestClass">$(Escape-Html $latestStatus)</div></div>
      <div class="card"><div class="label">Duration</div><div class="value">$latestDuration</div></div>
    </div>
    <section>
      <h2>Latest Run Steps</h2>
      <table><thead><tr><th>Step</th><th>Command</th><th>Status</th><th>Duration</th><th>Error</th></tr></thead><tbody>$runRows</tbody></table>
    </section>
    <section>
      <h2>Recent Runs</h2>
      <table><thead><tr><th>Run ID</th><th>Status</th><th>Started</th><th>Duration</th></tr></thead><tbody>$recentRows</tbody></table>
    </section>
    <section>
      <h2>DolphinScheduler Template</h2>
      <pre>$(Escape-Html $workflowText)</pre>
    </section>
  </main>
</body>
</html>
"@

$dashboardPath = Join-Path $dashboardDir "index.html"
Set-Content -Path $dashboardPath -Value $html -Encoding UTF8
Write-Host "Generated scheduler dashboard at $dashboardPath"
