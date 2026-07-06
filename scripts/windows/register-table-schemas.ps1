$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $root

$registry = "http://127.0.0.1:8081/apis/registry/v3"
$entries = Get-Content -Raw (Join-Path $root "schemas/registry-manifest.json") | ConvertFrom-Json

function Normalize-Json([string]$Json) {
  return ($Json | ConvertFrom-Json | ConvertTo-Json -Depth 100 -Compress)
}

function New-VersionBody($Entry, [string]$Schema, [string]$Version) {
  [ordered]@{
    version = $Version
    name = "$($Entry.artifactId)-$Version"
    description = "Schema for $($Entry.source) object $($Entry.object)."
    content = @{ content = $Schema; contentType = "application/json"; references = @() }
    labels = @{ source = $Entry.source; object = $Entry.object }
  }
}

Invoke-RestMethod -Uri "$registry/system/info" -TimeoutSec 10 | Out-Null

foreach ($entry in $entries) {
  $schema = Get-Content -Raw (Join-Path (Join-Path $root "schemas") $entry.file)
  $artifactUri = "$registry/groups/$($entry.groupId)/artifacts/$($entry.artifactId)"
  $exists = $true
  try {
    $metadata = Invoke-RestMethod -Uri $artifactUri -TimeoutSec 10
  } catch {
    $statusCode = if ($_.Exception.Response) { [int]$_.Exception.Response.StatusCode } else { $null }
    if ($statusCode -ne 404) { throw }
    $exists = $false
  }

  if (-not $exists) {
    $body = [ordered]@{
      artifactId = $entry.artifactId
      artifactType = "JSON"
      name = $entry.object
      description = "Versioned schema for $($entry.source) object $($entry.object)."
      labels = @{ source = $entry.source; object = $entry.object; project = "ustc-streaming-lakehouse-demo" }
      firstVersion = New-VersionBody $entry $schema "1.0.0"
    } | ConvertTo-Json -Depth 12
    Invoke-RestMethod -Uri "$registry/groups/$($entry.groupId)/artifacts" -Method Post -ContentType "application/json" -Body $body -TimeoutSec 20 | Out-Null
    Write-Host "CREATED   $($entry.groupId)/$($entry.artifactId) version=1.0.0"
    continue
  }

  $latestContent = Invoke-RestMethod -Uri "$artifactUri/versions/branch=latest/content" -TimeoutSec 10
  $latestJson = $latestContent | ConvertTo-Json -Depth 100 -Compress
  if ((Normalize-Json $schema) -eq (Normalize-Json $latestJson)) {
    Write-Host "UNCHANGED $($entry.groupId)/$($entry.artifactId) version=$($metadata.version)"
    continue
  }

  $versions = Invoke-RestMethod -Uri "$artifactUri/versions" -TimeoutSec 10
  $version = "1.$([int]$versions.count).0"
  $body = New-VersionBody $entry $schema $version | ConvertTo-Json -Depth 12
  Invoke-RestMethod -Uri "$artifactUri/versions" -Method Post -ContentType "application/json" -Body $body -TimeoutSec 20 | Out-Null
  Write-Host "UPDATED   $($entry.groupId)/$($entry.artifactId) version=$version"
}

Write-Host ""
Write-Host "Registry artifacts:"
(Invoke-RestMethod -Uri "$registry/search/artifacts?limit=100" -TimeoutSec 10).artifacts |
  Select-Object groupId, artifactId, artifactType, version |
  Format-Table -AutoSize
