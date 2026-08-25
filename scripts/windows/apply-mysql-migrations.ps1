$ErrorActionPreference = "Continue"
if ($PSVersionTable.PSVersion.Major -ge 7) {
  $PSNativeCommandUseErrorActionPreference = $false
}
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $root

$migrationFiles = @(
  "mysql/migrations/029_expand_order_lifecycle.sql",
  "mysql/migrations/030_normalize_order_amount_fields.sql",
  "mysql/migrations/031_add_order_shop_id.sql",
  "mysql/migrations/032_enrich_creative_config.sql",
  "mysql/migrations/034_repair_utf8_master_data.sql",
  "mysql/migrations/035_remove_order_ad_source_fields.sql",
  "mysql/migrations/036_simplify_order_lifecycle.sql",
  "mysql/migrations/037_rename_unit_delivery_type.sql"
)
$deadline = (Get-Date).AddMinutes(2)
do {
  $ready = docker compose exec -T mysql mysqladmin -uroot -proot ping --silent 2>$null
  if ($LASTEXITCODE -eq 0) {
    foreach ($migrationFile in $migrationFiles) {
      $containerFile = "/tmp/" + (Split-Path $migrationFile -Leaf)
      docker cp $migrationFile "ustc_lakehouse-mysql-1:$containerFile"
      if ($LASTEXITCODE -ne 0) { throw "Copying MySQL migration failed: $migrationFile" }
      docker compose exec -T mysql /bin/bash -lc "mysql -uroot -proot --default-character-set=utf8mb4 < '$containerFile'"
      if ($LASTEXITCODE -ne 0) { throw "MySQL migration failed: $migrationFile" }
      Write-Host "Applied $migrationFile"
    }
    exit 0
  }

  if ((Get-Date) -ge $deadline) {
    throw "MySQL did not become ready within 2 minutes."
  }

  Write-Host "Waiting for MySQL before applying lifecycle migration..."
  Start-Sleep -Seconds 2
} while ($true)
