$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $root
$sql = Get-Content -Raw (Join-Path $root "starrocks\init_starrocks.sql")
$sql | docker compose exec -T starrocks mysql --protocol=TCP --host=127.0.0.1 --port=9030 --user=root
if ($LASTEXITCODE -ne 0) { throw "StarRocks initialization failed" }
Write-Host "StarRocks realtime and daily serving tables are ready."
