$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$lib = Join-Path $root "flink\lib"
New-Item -ItemType Directory -Force -Path $lib | Out-Null
$jars = @(
  @{ Name = "paimon-flink-1.20.jar"; Url = "https://repo1.maven.org/maven2/org/apache/paimon/paimon-flink-1.20/1.3.1/paimon-flink-1.20-1.3.1.jar" },
  @{ Name = "fluss-flink-1.20.jar"; Url = "https://repo1.maven.org/maven2/org/apache/fluss/fluss-flink-1.20/0.9.1-incubating/fluss-flink-1.20-0.9.1-incubating.jar" },
  @{ Name = "fluss-flink-tiering.jar"; Url = "https://repo1.maven.org/maven2/org/apache/fluss/fluss-flink-tiering/0.9.1-incubating/fluss-flink-tiering-0.9.1-incubating.jar" },
  @{ Name = "fluss-lake-paimon.jar"; Url = "https://repo1.maven.org/maven2/org/apache/fluss/fluss-lake-paimon/0.9.1-incubating/fluss-lake-paimon-0.9.1-incubating.jar" },
  @{ Name = "flink-sql-connector-mysql-cdc-3.6.0-1.20.jar"; Url = "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-mysql-cdc/3.6.0-1.20/flink-sql-connector-mysql-cdc-3.6.0-1.20.jar" },
  @{ Name = "mysql-connector-j.jar"; Url = "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/9.3.0/mysql-connector-j-9.3.0.jar" },
  @{ Name = "flink-connector-starrocks.jar"; Url = "https://repo1.maven.org/maven2/com/starrocks/flink-connector-starrocks/1.2.12_flink-1.20/flink-connector-starrocks-1.2.12_flink-1.20.jar" },
  @{ Name = "hadoop-client-api.jar"; Url = "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.6/hadoop-client-api-3.3.6.jar" },
  @{ Name = "hadoop-client-runtime.jar"; Url = "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.3.6/hadoop-client-runtime-3.3.6.jar" }
)
foreach ($jar in $jars) {
  $target = Join-Path $lib $jar.Name
  if ((Test-Path $target -PathType Leaf) -and ((Get-Item $target).Length -gt 0)) { Write-Host "exists $($jar.Name)"; continue }
  Invoke-WebRequest -Uri $jar.Url -OutFile $target -UseBasicParsing
  if ((Get-Item $target).Length -eq 0) { throw "Downloaded file is empty: $target" }
  Write-Host "downloaded $($jar.Name)"
}
