$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $root

docker compose --profile core --profile olap --profile bi --profile ops --profile governance --profile metastore --profile scheduler down
