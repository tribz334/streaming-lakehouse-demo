$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$lib = Join-Path $root "flink\lib"
New-Item -ItemType Directory -Force -Path $lib | Out-Null

$jars = @(
  @{
    Name = "paimon-flink-2.0.jar"
    Url = "https://repo1.maven.org/maven2/org/apache/paimon/paimon-flink-2.0/1.1.1/paimon-flink-2.0-1.1.1.jar"
  },
  @{
    Name = "flink-sql-connector-kafka.jar"
    Url = "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.1-2.0/flink-sql-connector-kafka-4.0.1-2.0.jar"
  },
  @{
    Name = "flink-sql-connector-mysql-cdc.jar"
    Url = "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-mysql-cdc/3.5.0/flink-sql-connector-mysql-cdc-3.5.0.jar"
  },
  @{
    Name = "mysql-connector-j.jar"
    Url = "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/9.3.0/mysql-connector-j-9.3.0.jar"
  },
  @{
    Name = "flink-shaded-hadoop-2-uber.jar"
    Url = "https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar"
  },
  @{
    Name = "flink-connector-jdbc-core.jar"
    Url = "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc-core/4.0.0-2.0/flink-connector-jdbc-core-4.0.0-2.0.jar"
  },
  @{
    Name = "flink-connector-jdbc-mysql.jar"
    Url = "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc-mysql/4.0.0-2.0/flink-connector-jdbc-mysql-4.0.0-2.0.jar"
  }
)

foreach ($jar in $jars) {
  $target = Join-Path $lib $jar.Name
  if (Test-Path $target -PathType Leaf) {
    Write-Host "exists $($jar.Name)"
    continue
  }
  for ($i = 1; $i -le 6; $i++) {
    try {
      Write-Host ("Downloading {0}, attempt {1}" -f $jar.Name, $i)
      Invoke-WebRequest -Uri $jar.Url -OutFile $target -UseBasicParsing
      break
    } catch {
      if ($i -eq 6) { throw }
      Start-Sleep -Seconds (3 * $i)
    }
  }
}
