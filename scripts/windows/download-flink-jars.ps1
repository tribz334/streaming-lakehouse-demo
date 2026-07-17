$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$lib = Join-Path $root "flink\lib"
New-Item -ItemType Directory -Force -Path $lib | Out-Null

$jars = @(
  @{
    Name = "paimon-flink-2.2.jar"
    Url = "https://repo1.maven.org/maven2/org/apache/paimon/paimon-flink-2.2/1.4.2/paimon-flink-2.2-1.4.2.jar"
  },
  @{
    Name = "hive-apache-1.2.2-2.jar"
    Url = "https://repo1.maven.org/maven2/com/facebook/presto/hive/hive-apache/1.2.2-2/hive-apache-1.2.2-2.jar"
  },
  @{
    Name = "flink-sql-connector-kafka-5.0.0-2.2.jar"
    Url = "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar"
  },
  @{
    Name = "flink-sql-connector-mysql-cdc-3.6.0-2.2.jar"
    Url = "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-mysql-cdc/3.6.0-2.2/flink-sql-connector-mysql-cdc-3.6.0-2.2.jar"
  },
  @{
    Name = "flink-cdc-pipeline-connector-mysql-3.6.0-2.2.jar"
    Url = "https://repo1.maven.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-mysql/3.6.0-2.2/flink-cdc-pipeline-connector-mysql-3.6.0-2.2.jar"
  },
  @{
    Name = "flink-cdc-pipeline-connector-paimon-3.6.0-2.2.jar"
    Url = "https://repo1.maven.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-paimon/3.6.0-2.2/flink-cdc-pipeline-connector-paimon-3.6.0-2.2.jar"
  },
  @{
    Name = "flink-cdc-3.6.0-2.2-bin.tar.gz"
    Url = "https://downloads.apache.org/flink/flink-cdc-3.6.0/flink-cdc-3.6.0-2.2-bin.tar.gz"
  },
  @{
    Name = "mysql-connector-j.jar"
    Url = "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/9.3.0/mysql-connector-j-9.3.0.jar"
  },
  @{
    Name = "flink-shaded-hadoop-2-uber.jar"
    Url = "https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar"
  }
)

foreach ($jar in $jars) {
  $target = Join-Path $lib $jar.Name
  if ((Test-Path $target -PathType Leaf) -and ((Get-Item $target).Length -gt 0)) {
    Write-Host "exists $($jar.Name)"
    continue
  }
  if (Test-Path $target -PathType Leaf) {
    Remove-Item -LiteralPath $target -Force
  }
  for ($i = 1; $i -le 6; $i++) {
    try {
      Write-Host ("Downloading {0}, attempt {1}" -f $jar.Name, $i)
      curl.exe --fail --location --connect-timeout 20 --max-time 600 --output $target $jar.Url
      if ($LASTEXITCODE -ne 0) {
        throw "curl failed with exit code $LASTEXITCODE"
      }
      if (-not (Test-Path $target -PathType Leaf) -or ((Get-Item $target).Length -eq 0)) {
        throw "Downloaded file is empty: $target"
      }
      break
    } catch {
      Remove-Item -LiteralPath $target -Force -ErrorAction SilentlyContinue
      if ($i -eq 6) { throw }
      Start-Sleep -Seconds (3 * $i)
    }
  }
}
