$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $root

$registry = "http://127.0.0.1:8081/apis/registry/v3"
$schemaPath = Join-Path $root "schemas/ods_log.schema.json"
$schema = Get-Content -Raw -Path $schemaPath
$artifactId = "ods_log-value"
$groupId = "ad-demo"
$artifactVersion = "10.0.0"
$eventSchemaVersion = "10"

$ready = $false
$deadline = (Get-Date).AddSeconds(90)
do {
  try {
    Invoke-RestMethod -Uri "$registry/system/info" -TimeoutSec 5 | Out-Null
    $ready = $true
  } catch {
    Start-Sleep -Seconds 3
  }
} while (-not $ready -and (Get-Date) -lt $deadline)

if (-not $ready) {
  throw "Apicurio Registry did not become ready within 90 seconds."
}

$exists = $false
try {
  Invoke-RestMethod -Uri "$registry/groups/$groupId/artifacts/$artifactId" -TimeoutSec 10 | Out-Null
  $exists = $true
} catch {
  $statusCode = $null
  if ($_.Exception.Response) {
    $statusCode = [int]$_.Exception.Response.StatusCode
  }
  if ($statusCode -and $statusCode -ne 404) {
    throw
  }
}

if (-not $exists) {
  $body = [ordered]@{
    artifactId = $artifactId
    artifactType = "JSON"
    name = "Kafka ods_log value schema"
    description = "JSON schema for the ad event stream written to Kafka topic ods_log."
    labels = @{
      layer = "ods"
      topic = "ods_log"
      project = "ustc-streaming-lakehouse-demo"
    }
    firstVersion = @{
      version = $artifactVersion
      name = "ods_log-value-$artifactVersion"
      description = "Advertising behavior-only schema; orders and billable cost are read from MySQL CDC."
      content = @{
        content = $schema
        contentType = "application/json"
        references = @()
      }
      labels = @{
        schema_version = $eventSchemaVersion
      }
    }
  } | ConvertTo-Json -Depth 12

  $uri = "$registry/groups/$groupId/artifacts"
  $result = Invoke-RestMethod -Uri $uri -Method Post -ContentType "application/json" -Body $body -TimeoutSec 20

  Write-Host "Registered Apicurio artifact:"
  $result | ConvertTo-Json -Depth 8
} else {
  $versionExists = $false
  try {
    Invoke-RestMethod -Uri "$registry/groups/$groupId/artifacts/$artifactId/versions/$artifactVersion" -TimeoutSec 10 | Out-Null
    $versionExists = $true
  } catch {
    $statusCode = $null
    if ($_.Exception.Response) {
      $statusCode = [int]$_.Exception.Response.StatusCode
    }
    if ($statusCode -and $statusCode -ne 404) {
      throw
    }
  }

  if ($versionExists) {
    Write-Host "Apicurio schema version already exists: $groupId/$artifactId@$artifactVersion"
  } else {
    $versionBody = [ordered]@{
      version = $artifactVersion
      name = "ods_log-value-$artifactVersion"
      description = "Advertising behavior-only schema; orders and billable cost are read from MySQL CDC."
      content = @{
        content = $schema
        contentType = "application/json"
        references = @()
      }
      labels = @{
        schema_version = $eventSchemaVersion
      }
    } | ConvertTo-Json -Depth 12

    $versionResult = Invoke-RestMethod `
      -Uri "$registry/groups/$groupId/artifacts/$artifactId/versions" `
      -Method Post -ContentType "application/json" -Body $versionBody -TimeoutSec 20
    Write-Host "Registered Apicurio schema version:"
    $versionResult | ConvertTo-Json -Depth 8
  }
}

Write-Host ""
Write-Host "Current versions for ${groupId}/${artifactId}:"
Invoke-RestMethod `
  -Uri "$registry/groups/$groupId/artifacts/$artifactId/versions?order=asc" `
  -TimeoutSec 10 | ConvertTo-Json -Depth 8
