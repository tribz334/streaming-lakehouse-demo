$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $root

docker compose --profile olap up -d starrocks starrocks-be
.\scripts\windows\init-starrocks.ps1
.\scripts\windows\sync-starrocks-olap.ps1
docker compose --profile olap --profile bi up -d superset
docker compose --profile olap --profile bi exec -T superset python /app/pythonpath/bootstrap_datasets.py

Write-Host "Superset requested: http://127.0.0.1:8088"
Write-Host "Default login: admin / admin"
