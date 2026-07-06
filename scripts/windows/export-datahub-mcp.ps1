$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $root

$metadataPath = Join-Path $root "datahub/metadata/lakehouse_metadata.json"
if (-not (Test-Path $metadataPath)) {
  ./scripts/windows/export-governance-metadata.ps1
}

$metadata = Get-Content -Raw -Path $metadataPath | ConvertFrom-Json
$outputDir = Join-Path $root "datahub/mcp"
New-Item -ItemType Directory -Force -Path $outputDir | Out-Null
$outputPath = Join-Path $outputDir "metadata_change_proposals.jsonl"

$lines = New-Object System.Collections.Generic.List[string]

foreach ($dataset in $metadata.datasets) {
  $datasetProperties = [ordered]@{
    description = "USTC streaming lakehouse demo dataset: $($dataset.name)"
    customProperties = [ordered]@{
      project = $metadata.project
      platform = $dataset.platform
      layer = $dataset.layer
      domain = if ($dataset.PSObject.Properties.Name -contains "domain") { $dataset.domain } else { "" }
    }
  }
  $lines.Add(([ordered]@{
    entityType = "dataset"
    entityUrn = $dataset.urn
    changeType = "UPSERT"
    aspectName = "datasetProperties"
    aspect = $datasetProperties
  } | ConvertTo-Json -Depth 8 -Compress))

  if ($dataset.PSObject.Properties.Name -contains "domain" -and $dataset.domain) {
    $globalTags = [ordered]@{
      tags = @(
        [ordered]@{ tag = "urn:li:tag:$($dataset.domain)" }
      )
    }
    $lines.Add(([ordered]@{
      entityType = "dataset"
      entityUrn = $dataset.urn
      changeType = "UPSERT"
      aspectName = "globalTags"
      aspect = $globalTags
    } | ConvertTo-Json -Depth 8 -Compress))
  }
}

$lineageByDownstream = @{}
foreach ($edge in $metadata.lineage) {
  if (-not $lineageByDownstream.ContainsKey($edge.downstream)) {
    $lineageByDownstream[$edge.downstream] = New-Object System.Collections.Generic.List[object]
  }
  $lineageByDownstream[$edge.downstream].Add([ordered]@{
    dataset = $edge.upstream
    type = "TRANSFORMED"
    auditStamp = [ordered]@{
      time = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
      actor = "urn:li:corpuser:local-demo"
    }
  })
}

foreach ($downstream in $lineageByDownstream.Keys) {
  $upstreams = @()
  foreach ($item in $lineageByDownstream[$downstream]) {
    $upstreams += $item
  }
  $upstreamLineage = [ordered]@{
    upstreams = $upstreams
  }
  $lines.Add(([ordered]@{
    entityType = "dataset"
    entityUrn = $downstream
    changeType = "UPSERT"
    aspectName = "upstreamLineage"
    aspect = $upstreamLineage
  } | ConvertTo-Json -Depth 12 -Compress))
}

foreach ($term in $metadata.glossary_terms) {
  $termUrn = "urn:li:glossaryTerm:$($term.term -replace '[^A-Za-z0-9]+', '_')"
  $glossaryTermInfo = [ordered]@{
    name = $term.term
    definition = "Local demo glossary term applied to $($term.applies_to)."
    termSource = "INTERNAL"
  }
  $lines.Add(([ordered]@{
    entityType = "glossaryTerm"
    entityUrn = $termUrn
    changeType = "UPSERT"
    aspectName = "glossaryTermInfo"
    aspect = $glossaryTermInfo
  } | ConvertTo-Json -Depth 8 -Compress))
}

$lines | Set-Content -Path $outputPath -Encoding UTF8
Write-Host "Exported DataHub MCP-style JSONL to $outputPath"
