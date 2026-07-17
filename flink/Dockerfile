ARG FLINK_BASE_IMAGE=flink:2.2.0-scala_2.12-java17
FROM ${FLINK_BASE_IMAGE}

ARG PAIMON_VERSION=1.4.2
ARG FLINK_KAFKA_CONNECTOR_VERSION=5.0.0-2.2
ARG FLINK_CDC_VERSION=3.6.0-2.2
ARG MYSQL_CONNECTOR_VERSION=9.3.0

USER root
COPY lib/paimon-flink-2.2.jar /opt/flink/lib/paimon-flink-2.2.jar
COPY lib/hive-apache-1.2.2-2.jar /opt/flink/lib/hive-apache.jar
COPY lib/flink-sql-connector-kafka-5.0.0-2.2.jar /opt/flink/lib/flink-sql-connector-kafka.jar
COPY lib/flink-sql-connector-mysql-cdc-3.6.0-2.2.jar /opt/flink/lib/flink-sql-connector-mysql-cdc.jar
COPY lib/mysql-connector-j.jar /opt/flink/lib/mysql-connector-j.jar
COPY lib/flink-shaded-hadoop-2-uber.jar /opt/flink/lib/flink-shaded-hadoop-2-uber.jar
COPY lib/flink-cdc-3.6.0-2.2-bin.tar.gz /tmp/flink-cdc.tar.gz
RUN tar -xzf /tmp/flink-cdc.tar.gz -C /opt \
    && mv /opt/flink-cdc-3.6.0-2.2 /opt/flink-cdc \
    && cp /opt/flink-cdc/lib/flink-cdc-dist-3.6.0-2.2.jar /opt/flink/lib/flink-cdc-dist.jar \
    && rm /tmp/flink-cdc.tar.gz
COPY lib/flink-cdc-pipeline-connector-mysql-3.6.0-2.2.jar /opt/flink-cdc/lib/flink-cdc-pipeline-connector-mysql.jar
COPY lib/flink-cdc-pipeline-connector-paimon-3.6.0-2.2.jar /opt/flink-cdc/lib/flink-cdc-pipeline-connector-paimon.jar
COPY lib/flink-cdc-pipeline-connector-mysql-3.6.0-2.2.jar /opt/flink/lib/flink-cdc-pipeline-connector-mysql.jar
COPY lib/flink-cdc-pipeline-connector-paimon-3.6.0-2.2.jar /opt/flink/lib/flink-cdc-pipeline-connector-paimon.jar

RUN mkdir -p /warehouse/paimon /opt/flink/usrlib/sql /opt/flink-cdc/pipelines \
    && chown -R flink:flink /warehouse /opt/flink/usrlib /opt/flink-cdc
USER flink
