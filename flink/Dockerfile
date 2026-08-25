ARG FLINK_BASE_IMAGE=flink:1.20.3-scala_2.12-java17
FROM ${FLINK_BASE_IMAGE}

USER root
COPY lib/paimon-flink-1.20.jar /opt/flink/lib/paimon-flink-1.20.jar
COPY lib/fluss-flink-1.20.jar /opt/flink/lib/fluss-flink-1.20.jar
COPY lib/fluss-flink-tiering.jar /opt/flink/opt/fluss-flink-tiering.jar
COPY lib/fluss-lake-paimon.jar /opt/flink/lib/fluss-lake-paimon.jar
COPY lib/flink-sql-connector-mysql-cdc-3.6.0-1.20.jar /opt/flink/lib/flink-sql-connector-mysql-cdc.jar
COPY lib/mysql-connector-j.jar /opt/flink/lib/mysql-connector-j.jar
COPY lib/flink-connector-starrocks.jar /opt/flink/lib/flink-connector-starrocks.jar
COPY lib/hadoop-client-api.jar /opt/flink/lib/hadoop-client-api.jar
COPY lib/hadoop-client-runtime.jar /opt/flink/lib/hadoop-client-runtime.jar
RUN mkdir -p /warehouse/fluss /warehouse/paimon /warehouse/checkpoints /opt/flink/usrlib/sql \
    && chown -R flink:flink /warehouse /opt/flink/usrlib
USER flink
