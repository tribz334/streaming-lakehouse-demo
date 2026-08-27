ARG FLINK_BASE_IMAGE=flink:1.20.3-scala_2.12-java17
FROM ${FLINK_BASE_IMAGE}

USER root
COPY flink/lib/paimon-flink-1.20.jar /opt/flink/lib/paimon-flink-1.20.jar
COPY flink/lib/fluss-flink-1.20.jar /opt/flink/lib/fluss-flink-1.20.jar
COPY flink/lib/fluss-flink-tiering.jar /opt/flink/opt/fluss-flink-tiering.jar
COPY flink/lib/fluss-lake-paimon.jar /opt/flink/lib/fluss-lake-paimon.jar
COPY flink/lib/flink-sql-connector-mysql-cdc-3.6.0-1.20.jar /opt/flink/lib/flink-sql-connector-mysql-cdc.jar
COPY flink/lib/mysql-connector-j.jar /opt/flink/lib/mysql-connector-j.jar
COPY flink/lib/flink-connector-starrocks.jar /opt/flink/lib/flink-connector-starrocks.jar
COPY flink/lib/hadoop-client-api.jar /opt/flink/lib/hadoop-client-api.jar
COPY flink/lib/hadoop-client-runtime.jar /opt/flink/lib/hadoop-client-runtime.jar
COPY flink/lib/jackson-annotations-2.18.3.jar /opt/flink/lib/
COPY flink/lib/jackson-core-2.18.3.jar /opt/flink/lib/
COPY flink/lib/jackson-databind-2.18.3.jar /opt/flink/lib/
COPY flink-java/target/ad-realtime-datastream-jobs.jar /opt/flink/usrlib/ad-realtime-datastream-jobs.jar
RUN mkdir -p /warehouse/fluss /warehouse/paimon /warehouse/checkpoints /opt/flink/usrlib/sql \
    && chown -R flink:flink /warehouse /opt/flink/usrlib
USER flink
